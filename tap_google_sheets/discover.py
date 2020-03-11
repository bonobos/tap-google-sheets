from singer.catalog import Catalog, CatalogEntry, Schema

from tap_google_sheets.schema import get_schemas, STREAMS


def select_all_fields_in_streams(catalog):
    for stream in catalog.streams:
        for metadata in stream.metadata:
            metadata['metadata'].update({'selected': True})


def discover(select_all, client, spreadsheet_id):
    schemas, field_metadata = get_schemas(client, spreadsheet_id)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict, selected=select_all)
        mdata = field_metadata[stream_name]
        key_properties = None
        for mdt in mdata:
            table_key_properties = mdt.get('metadata', {}).get('table-key-properties')
            if table_key_properties:
                key_properties = table_key_properties

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=STREAMS.get(stream_name, {}).get('key_properties', key_properties),
            schema=schema,
            metadata=mdata
        ))

    if select_all:
        select_all_fields_in_streams(catalog)

    return catalog
