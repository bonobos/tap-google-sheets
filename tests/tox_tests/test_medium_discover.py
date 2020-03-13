from tap_beproduct.discover import discover


def test_discover_none_selected():
    result = discover(select_all=False)
    assert_selected(result, all_selected=False)


def test_discover_all_selected():
    result = discover(select_all=True)
    assert_selected(result, all_selected=True)


def assert_selected(catalog, all_selected):
    for stream in catalog.streams:
        assert stream.schema.selected == all_selected
        for metadata in stream.metadata:
            if all_selected:
                assert metadata.get('metadata').get('selected') == all_selected
            else:
                assert 'selected' not in metadata.get('metadata')
