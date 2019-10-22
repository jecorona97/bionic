import pytest
import pickle
from pathlib2 import Path
from subprocess import check_call
from ..helpers import skip_unless_gcs, GCS_TEST_BUCKET, df_from_csv_str, equal_frame_and_index_content

import bionic as bn
import dask.dataframe as dd

@pytest.fixture(scope='function')
def preset_builder(builder):
    builder.assign('x', 2)
    builder.assign('y', 3)

    @builder
    def f(x, y):
        return x + y

    return builder


@pytest.fixture(scope='function')
def flow(preset_builder):
    return preset_builder.build()


def test_copy_filename(flow):
    assert pickle.loads(flow.get('f', mode='FileCopier')
                        .src_file_path
                        .read_bytes()) == 5


def test_copy_to_existing_directory(flow, tmp_path):
    dir_path = tmp_path / 'output'
    dir_path.mkdir()
    flow.get('f', mode='FileCopier').new_copy(destination=dir_path)

    expected_file_path = dir_path / 'f.pkl'
    assert pickle.loads(expected_file_path.read_bytes()) == 5


def test_copy_to_file(flow, tmp_path):
    file_path = tmp_path / 'data.pkl'
    flow.get('f', mode='FileCopier').new_copy(destination=file_path)

    assert pickle.loads(file_path.read_bytes()) == 5


def test_copy_to_file_using_str(flow, tmp_path):
    file_path = tmp_path / 'data.pkl'
    file_path_str = str(file_path)
    flow.get('f', mode='FileCopier').new_copy(destination=file_path_str)
    assert pickle.loads(file_path.read_bytes()) == 5



@pytest.mark.parametrize('src_is_dir', [True])
@pytest.mark.parametrize('dest_is_dir', [True])
# @pytest.mark.parametrize('is_gcs', [True, False])
def test_copy_using_dask(builder, tmp_path, src_is_dir, dest_is_dir):

    import pandas.testing as pdt
    df_value = df_from_csv_str('''
    color,number
    red,1
    blue,2
    green,3
    ''')
    dask_df_val = dd.from_pandas(df_value, npartitions=1)

    @builder
    @bn.protocol.dask
    def dask_df():
        return dask_df_val

    @builder
    @bn.protocol.frame
    def df():
        return df_value

    flow = builder.build()

    if src_is_dir:
        entity_name = 'dask_df'
        expected = dask_df_val
    else:
        entity_name = 'df'
        expected = df_value

    if dest_is_dir:
        destination = tmp_path / 'output'
        destination.mkdir()
        # destination = destination / 'df.pq'
    else:
        destination = tmp_path / 'data'

    print(entity_name)
    print(expected)
    print(destination)
    print(type(destination))
    flow.get(entity_name, mode='FileCopier').new_copy(destination=destination)
    if src_is_dir:
        print('hi')
        actual = dd.read_parquet(destination / 'dask_df.pq.dask')
        print(type(actual))
        print(actual)
        assert equal_frame_and_index_content(actual.compute(), expected.compute())
    else:
        #actual = pickle.loads(destination.read_bytes())
        from pyarrow import parquet
        with destination.open('rb') as ifp:
            actual = parquet.read_table(ifp).to_pandas()
        print(actual)
    #pdt.assert_frame_equal(actual, expected)


def test_copy_dask_to_dir(builder, tmp_path):

    import pandas.testing as pdt
    df_value = df_from_csv_str('''
    color,number
    red,1
    blue,2
    green,3
    ''')
    dask_df_val = dd.from_pandas(df_value, npartitions=1)

    @builder
    @bn.protocol.dask
    def dask_df():
        return dask_df_val

    @builder
    @bn.protocol.frame
    def df():
        return df_value

    flow = builder.build()

    if src_is_dir:
        entity_name = 'dask_df'
        expected = dask_df_val
    else:
        entity_name = 'df'
        expected = df_value

    if dest_is_dir:
        destination = tmp_path / 'output'
        destination.mkdir()
        # destination = destination / 'df.pq'
    else:
        destination = tmp_path / 'data'

    print(entity_name)
    print(expected)
    print(destination)
    print(type(destination))
    flow.get(entity_name, mode='FileCopier').new_copy(destination=destination)
    if src_is_dir:
        print('hi')
        actual = dd.read_parquet(destination / 'dask_df.pq.dask')
        print(type(actual))
        print(actual)
        assert equal_frame_and_index_content(actual.compute(), expected.compute()) 

def test_copy_file_to_dir():
    pass

def test_copy_file_to_file():
    pass

# TODO: ADD TEST FOR MULTI-VALUE EXPORT


@skip_unless_gcs
def test_copy_to_gcs_dir(flow, tmp_path):
    flow.get('f', mode='FileCopier').new_copy(destination='gs://' + GCS_TEST_BUCKET)
    src = Path(GCS_TEST_BUCKET) / 'f.pkl'
    dst = tmp_path / 'f.pkl'
    check_call('gsutil -m cp gs://{} {}'.format(src, dst), shell=True)
    assert pickle.loads(dst.read_bytes()) == 5
    check_call('gsutil -m rm gs://{}'.format(src), shell=True)


@skip_unless_gcs
def test_copy_to_gcs_file(flow, tmp_path):
    src = str(Path(GCS_TEST_BUCKET) / 'f.pkl')
    flow.get('f', mode='FileCopier').new_copy(destination='gs://' + src)
    dst = tmp_path / 'f.pkl'
    check_call('gsutil -m cp gs://{} {}'.format(src, dst), shell=True)
    assert pickle.loads(dst.read_bytes()) == 5
    check_call('gsutil -m rm gs://{}'.format(src), shell=True)
