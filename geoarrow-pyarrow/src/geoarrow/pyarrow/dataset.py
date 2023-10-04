"""
Experimental geospatial-agumented wrapper around a ``pyarrow.dataset``.

>>> import geoarrow.pyarrow.dataset as gads
"""

from concurrent.futures import ThreadPoolExecutor, wait

import pyarrow as _pa
import pyarrow.types as _types
import pyarrow.dataset as _ds
import pyarrow.compute as _compute
import pyarrow.parquet as _pq
from geoarrow.c.lib import CoordType
from geoarrow.pyarrow._type import wkt, wkb, GeometryExtensionType
from geoarrow.pyarrow._kernel import Kernel


class GeoDataset:
    """Geospatial-augmented Dataset

    EXPERIMENTAL

    The GeoDataset wraps a pyarrow.Dataset containing one or more geometry columns
    and provides indexing and IO capability. If `geometry_columns` is `None`,
    it will include all columns that inherit from `geoarrow.pyarrow.GeometryExtensionType`.
    The `geometry_columns` are not required to be geoarrow extension type columns:
    text columns will be parsed as WKT; binary columns will be parsed as WKB
    (but are not detected automatically).

    Note that the ``GeoDataset`` is only useful in a context where each fragment
    has been written such that features in the fragment are close together
    in space (e.g., one file or row group per state).
    """

    def __init__(self, parent, geometry_columns=None):
        self._index = None
        self._geometry_columns = geometry_columns
        self._geometry_types = None
        self._fragments = None

        if not isinstance(parent, _ds.Dataset):
            raise TypeError("parent must be a pyarrow.dataset.Dataset")
        self._parent = parent

    @property
    def parent(self):
        """Get the parent Dataset

        Returns the (non geo-aware) parent pyarrow.Dataset.

        >>> import geoarrow.pyarrow.dataset as gads
        >>> import geoarrow.pyarrow as ga
        >>> import pyarrow as pa
        >>> table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
        >>> dataset = gads.dataset(table)
        >>> type(dataset.parent)
        <class 'pyarrow._dataset.InMemoryDataset'>
        """
        return self._parent

    def to_table(self):
        return self.parent.to_table()

    @property
    def schema(self):
        """Get the dataset schema

        The schema of a GeoDataset is identical to that of its parent.

        >>> import geoarrow.pyarrow.dataset as gads
        >>> import geoarrow.pyarrow as ga
        >>> import pyarrow as pa
        >>> table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
        >>> dataset = gads.dataset(table)
        >>> dataset.schema
        geometry: extension<geoarrow.wkt<WktType>>
        """
        return self._parent.schema

    def get_fragments(self):
        """Resolve the list of fragments in the dataset

        This is identical to the list of fragments of its parent."""
        if self._fragments is None:
            self._fragments = tuple(self._parent.get_fragments())

        return self._fragments

    @property
    def geometry_columns(self):
        """Get a tuple of geometry column names

        >>> import geoarrow.pyarrow.dataset as gads
        >>> import geoarrow.pyarrow as ga
        >>> import pyarrow as pa
        >>> table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
        >>> dataset = gads.dataset(table)
        >>> dataset.geometry_columns
        ('geometry',)
        """
        if self._geometry_columns is None:
            schema = self.schema
            geometry_columns = []
            for name, type in zip(schema.names, schema.types):
                if isinstance(type, GeometryExtensionType):
                    geometry_columns.append(name)
            self._geometry_columns = tuple(geometry_columns)

        return self._geometry_columns

    @property
    def geometry_types(self):
        """Resolve a tuple of geometry column types

        This will convert any primitive types to the corresponding
        geo-enabled type (e.g., binary to wkb) and check that geometry
        columns actually refer a field that can be interpreted as
        geometry.

        >>> import geoarrow.pyarrow.dataset as gads
        >>> import geoarrow.pyarrow as ga
        >>> import pyarrow as pa
        >>> table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
        >>> dataset = gads.dataset(table)
        >>> dataset.geometry_types
        (WktType(geoarrow.wkt),)
        """
        if self._geometry_types is None:
            geometry_types = []
            for col in self.geometry_columns:
                type = self.schema.field(col).type
                if isinstance(type, GeometryExtensionType):
                    geometry_types.append(type)
                elif _types.is_binary(type):
                    geometry_types.append(wkb())
                elif _types.is_string(type):
                    geometry_types.append(wkt())
                else:
                    raise TypeError(f"Unsupported type for geometry column: {type}")

            self._geometry_types = tuple(geometry_types)

        return self._geometry_types

    def index_fragments(self, num_threads=None):
        """Resolve a simplified geometry for each fragment

        Currently the simplified geometry is a box in the form of a
        struct array with fields xmin, xmax, ymin, and ymax. The
        fragment index is curently a table whose first column is the fragment
        index and whose subsequent columns are named with the geometry column
        name. A future implementation may handle spherical edges using a type
        of simplified geometry more suitable to a spherical comparison.

        >>> import geoarrow.pyarrow.dataset as gads
        >>> import geoarrow.pyarrow as ga
        >>> import pyarrow as pa
        >>> table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
        >>> dataset = gads.dataset(table)
        >>> dataset.index_fragments().to_pylist()
        [{'_fragment_index': 0, 'geometry': {'xmin': 0.5, 'xmax': 0.5, 'ymin': 1.5, 'ymax': 1.5}}]
        """
        if self._index is None:
            self._index = self._build_index(
                self.geometry_columns, self.geometry_types, num_threads
            )

        return self._index

    def _build_index(self, geometry_columns, geometry_types, num_threads=None):
        return GeoDataset._index_fragments(
            self.get_fragments(),
            geometry_columns,
            geometry_types,
            num_threads=num_threads,
        )

    def filter_fragments(self, target):
        """Push down a spatial query into a GeoDataset

        Returns a potentially simplified dataset based on the geometry of
        target. Currently this uses `geoarrow.pyarrow.box_agg()` on `target`
        and performs a simple envelope comparison with each fragment. A future
        implementation may handle spherical edges using a type of simplified
        geometry more suitable to a spherical comparison. For datasets with
        more than one geometry column, the filter will be applied to all columns
        and include fragments that intersect the simplified geometry from any
        of the columns.

        Note that datasets with large row groups/fragments and/or datasets that
        were not written with fragments with spatial significance may return
        most or all of the fragments in the parent dataset.

        >>> import geoarrow.pyarrow.dataset as gads
        >>> import geoarrow.pyarrow as ga
        >>> import pyarrow as pa
        >>> table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
        >>> dataset = gads.dataset(table)
        >>> dataset.filter_fragments("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))").to_table()
        pyarrow.Table
        geometry: extension<geoarrow.wkt<WktType>>
        ----
        geometry: []
        >>> dataset.filter_fragments("POLYGON ((0 1, 0 2, 1 2, 1 1, 0 1))").to_table()
        pyarrow.Table
        geometry: extension<geoarrow.wkt<WktType>>
        ----
        geometry: [["POINT (0.5 1.5)"]]
        """
        from ._compute import box_agg

        if isinstance(target, str):
            target = [target]
        target_box = box_agg(target)
        maybe_intersects = GeoDataset._index_box_intersects(
            self.index_fragments(), target_box, self.geometry_columns
        )
        fragment_indices = [scalar.as_py() for scalar in maybe_intersects]
        filtered_parent = self._filter_parent_fragment_indices(fragment_indices)
        return self._wrap_parent(filtered_parent, fragment_indices)

    def _wrap_parent(self, filtered_parent, fragment_indices):
        new_wrapped = GeoDataset(
            filtered_parent, geometry_columns=self._geometry_columns
        )
        new_wrapped._geometry_types = self.geometry_types

        new_index = self.index_fragments().take(
            _pa.array(fragment_indices, type=_pa.int64())
        )
        new_wrapped._index = new_index.set_column(
            0, "_fragment_index", _pa.array(range(new_index.num_rows))
        )

        return new_wrapped

    def _filter_parent_fragment_indices(self, fragment_indices):
        fragments = self.get_fragments()
        fragments_filtered = [fragments[i] for i in fragment_indices]

        if isinstance(self._parent, _ds.FileSystemDataset):
            return _ds.FileSystemDataset(
                fragments_filtered, self.schema, self._parent.format
            )
        else:
            tables = [fragment.to_table() for fragment in fragments_filtered]
            return _ds.InMemoryDataset(tables, schema=self.schema)

    @staticmethod
    def _index_fragment(fragment, column, type):
        scanner = fragment.scanner(columns=[column])
        reader = scanner.to_reader()
        kernel = Kernel.box_agg(type)
        for batch in reader:
            kernel.push(batch.column(0))
        return kernel.finish()

    @staticmethod
    def _index_fragments(fragments, columns, types, num_threads=None):
        columns = list(columns)
        if num_threads is None:
            num_threads = _pa.cpu_count()

        num_fragments = len(fragments)
        metadata = [_pa.array(range(num_fragments))]
        if not columns:
            return _pa.table(metadata, names=["_fragment_index"])

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for column, type in zip(columns, types):
                for fragment in fragments:
                    future = executor.submit(
                        GeoDataset._index_fragment, fragment, column, type
                    )
                    futures.append(future)

            wait(futures)

            results = []
            for i, column in enumerate(columns):
                results.append(
                    [
                        futures[i * num_fragments + j].result()
                        for j in range(num_fragments)
                    ]
                )

            result_arrays = [_pa.concat_arrays(result) for result in results]
            return _pa.table(
                metadata + result_arrays, names=["_fragment_index"] + columns
            )

    @staticmethod
    def _index_box_intersects(index, box, columns):
        xmin, xmax, ymin, ymax = box.as_py().values()
        expressions = []
        for col in columns:
            expr = (
                (_compute.field(col, "xmin") <= xmax)
                & (_compute.field(col, "xmax") >= xmin)
                & (_compute.field(col, "ymin") <= ymax)
                & (_compute.field(col, "ymax") >= ymin)
            )
            expressions.append(expr)

        expr = expressions[0]
        for i in range(1, len(expressions)):
            expr = expr | expressions[i]

        result = _ds.dataset(index).filter(expr).to_table()
        return result.column(0)


class ParquetRowGroupGeoDataset(GeoDataset):
    """Geospatial-augmented Parquet dataset using row groups

    An implementation of the GeoDataset that can leverage potentially
    more efficient indexing and more specific filtering. Notably, this
    implementation can (1) split a Parquet dataset into potentially more
    smaller fragments and (2) use column statistics added by most Parquet
    writers to more efficiently build the fragment index for types that support
    this capability.
    """

    def __init__(
        self,
        parent,
        row_group_fragments,
        row_group_ids,
        geometry_columns=None,
        use_column_statistics=True,
    ):
        super().__init__(parent, geometry_columns=geometry_columns)
        self._fragments = row_group_fragments
        self._row_group_ids = row_group_ids
        self._use_column_statistics = use_column_statistics

    @staticmethod
    def create(parent, geometry_columns=None, use_column_statistics=True):
        if not isinstance(parent, _ds.FileSystemDataset) or not isinstance(
            parent.format, _ds.ParquetFileFormat
        ):
            raise TypeError(
                "ParquetRowGroupGeoDataset() is only supported for Parquet datasets"
            )

        row_group_fragments = []
        row_group_ids = []

        for file_fragment in parent.get_fragments():
            for i, row_group_fragment in enumerate(file_fragment.split_by_row_group()):
                row_group_fragments.append(row_group_fragment)
                # Keep track of the row group IDs so we can accellerate
                # building an index later where column statistics are supported
                row_group_ids.append(i)

        parent = _ds.FileSystemDataset(
            row_group_fragments, parent.schema, parent.format
        )
        return ParquetRowGroupGeoDataset(
            parent,
            row_group_fragments,
            row_group_ids,
            geometry_columns=geometry_columns,
            use_column_statistics=use_column_statistics,
        )

    def _wrap_parent(self, filtered_parent, fragment_indices):
        base_wrapped = super()._wrap_parent(filtered_parent, fragment_indices)

        new_row_group_fragments = [self._fragments[i] for i in fragment_indices]
        new_row_group_ids = [self._row_group_ids[i] for i in fragment_indices]
        new_wrapped = ParquetRowGroupGeoDataset(
            base_wrapped._parent,
            new_row_group_fragments,
            new_row_group_ids,
            geometry_columns=base_wrapped.geometry_columns,
            use_column_statistics=self._use_column_statistics,
        )
        new_wrapped._index = base_wrapped._index
        return new_wrapped

    def _build_index(self, geometry_columns, geometry_types, num_threads=None):
        can_use_statistics = [
            type.coord_type == CoordType.SEPARATE for type in self.geometry_types
        ]

        if not self._use_column_statistics or not any(can_use_statistics):
            return super()._build_index(geometry_columns, geometry_types, num_threads)

        # Build a list of columns that will work with column stats
        bbox_stats_cols = []
        for col, use_stats in zip(geometry_columns, can_use_statistics):
            if use_stats:
                bbox_stats_cols.append(col)

        # Compute the column stats
        bbox_stats = self._build_index_using_stats(bbox_stats_cols)
        normal_stats_cols = list(geometry_columns)
        stats_by_name = {}
        for col, stat in zip(bbox_stats_cols, bbox_stats):
            # stat will contain nulls if any statistics were missing:
            if stat.null_count == 0:
                stats_by_name[col] = stat
                normal_stats_cols.remove(col)

        # Compute any remaining statistics
        normal_stats = super()._build_index(
            normal_stats_cols, geometry_types, num_threads
        )
        for col in normal_stats_cols:
            stats_by_name[col] = normal_stats.column(col)

        # Reorder stats to match the order of geometry_columns
        stat_cols = [stats_by_name[col] for col in geometry_columns]
        return _pa.table(
            [normal_stats.column(0)] + stat_cols,
            names=["_fragment_index"] + list(geometry_columns),
        )

    def _build_index_using_stats(self, geometry_columns):
        parquet_fields_before = ParquetRowGroupGeoDataset._count_fields_before(
            self.schema
        )
        parquet_fields_before = {k: v for k, v in parquet_fields_before}
        parquet_fields_before = [
            parquet_fields_before[(col,)] for col in geometry_columns
        ]
        return self._parquet_field_boxes(parquet_fields_before)

    def _parquet_field_boxes(self, parquet_indices):
        boxes = [[]] * len(parquet_indices)
        pq_file = None
        last_row_group = None

        # Note: probably worth parallelizing by file
        for row_group, fragment in zip(self._row_group_ids, self.get_fragments()):
            if pq_file is None or row_group < last_row_group:
                pq_file = _pq.ParquetFile(
                    fragment.path, filesystem=self._parent.filesystem
                )

            metadata = pq_file.metadata.row_group(row_group)
            for i, parquet_index in enumerate(parquet_indices):
                stats_x = metadata.column(parquet_index).statistics
                stats_y = metadata.column(parquet_index + 1).statistics

                if stats_x is None or stats_y is None:
                    boxes[i].append(None)
                else:
                    boxes[i].append(
                        {
                            "xmin": stats_x.min,
                            "xmax": stats_x.max,
                            "ymin": stats_y.min,
                            "ymax": stats_y.max,
                        }
                    )

            last_row_group = row_group

        type_field_names = ["xmin", "xmax", "ymin", "ymax"]
        type_fields = [_pa.field(name, _pa.float64()) for name in type_field_names]
        type = _pa.struct(type_fields)
        return [_pa.array(box, type=type) for box in boxes]

    @staticmethod
    def _count_fields_before(field, fields_before=None, path=(), count=0):
        """Helper to find the parquet column index of a given field path"""

        if isinstance(field, _pa.Schema):
            fields_before = []
            for i in range(len(field.types)):
                count = ParquetRowGroupGeoDataset._count_fields_before(
                    field.field(i), fields_before, path, count
                )
            return fields_before

        if isinstance(field.type, _pa.ExtensionType):
            field = _pa.field(field.name, field.type.storage_type)

        if _types.is_nested(field.type):
            path = path + (field.name,)
            fields_before.append((path, count))
            for i in range(field.type.num_fields):
                count = ParquetRowGroupGeoDataset._count_fields_before(
                    field.type.field(i), fields_before, path, count
                )
            return count
        else:
            fields_before.append((path + (field.name,), count))
            return count + 1


# Use a lazy import here to avoid requiring pyarrow.dataset
def dataset(*args, geometry_columns=None, use_row_groups=None, **kwargs):
    """Construct a GeoDataset

    This constructor is intended to mirror `pyarrow.dataset()`, adding
    geo-specific arguments. See :class:`geoarrow.pyarrow.dataset.GeoDataset` for
    details.

    >>> import geoarrow.pyarrow.dataset as gads
    >>> import geoarrow.pyarrow as ga
    >>> import pyarrow as pa
    >>> table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
    >>> dataset = gads.dataset(table)
    """

    parent = _ds.dataset(*args, **kwargs)

    if use_row_groups is None:
        use_row_groups = isinstance(parent, _ds.FileSystemDataset) and isinstance(
            parent.format, _ds.ParquetFileFormat
        )
    if use_row_groups:
        return ParquetRowGroupGeoDataset.create(
            parent, geometry_columns=geometry_columns
        )
    else:
        return GeoDataset(parent, geometry_columns=geometry_columns)
