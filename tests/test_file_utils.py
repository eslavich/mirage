import pytest
import os
import io
import glob

from mirage.utils import file_utils


TEST_DATA_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "test_data",
    "file_utils"
)

S3_TEST_DATA_PATH = "s3://mirage-data/test-data/file-utils"


class MockS3Client:
    def get_object(self, bucket_name, key):
        assert self.object_exists(bucket_name, key)

        with open(self._get_path(key), "rb") as f:
            return io.BytesIO(f.read())

    def object_exists(self, bucket_name, key):
        if bucket_name != "mirage-data":
            return False

        if not key.startswith("test-data/file-utils"):
            return False

        return os.path.isfile(self._get_path(key))

    def prefix_exists(self, bucket_name, key_prefix):
        return any(self.iterate_keys(bucket_name, key_prefix))

    def iterate_keys(self, bucket_name, key_prefix):
        if bucket_name != "mirage-data":
            return

        for k in self._list_keys():
            if k.startswith(key_prefix):
                yield k

    def _get_path(self, key):
        return TEST_DATA_PATH + key.replace("test-data/file-utils", "")

    def _list_keys(self):
        paths = glob.glob(TEST_DATA_PATH + "/**", recursive=True)
        paths = [p for p in paths if os.path.isfile(p)]
        return [p.replace(TEST_DATA_PATH, "test-data/file-utils") for p in paths]


@pytest.fixture(autouse=True)
def monkey_patch_environ(monkeypatch):
    monkeypatch.setenv("FILE_UTILS_TEST_DATA", TEST_DATA_PATH)
    monkeypatch.setenv("S3_FILE_UTILS_TEST_DATA", S3_TEST_DATA_PATH)


@pytest.fixture(autouse=True)
def monkey_patch_s3_client(monkeypatch):
    monkeypatch.setattr(file_utils, "_S3_CLIENT", MockS3Client())


def test_open_filesystem():
    with file_utils.open(f"{TEST_DATA_PATH}/test.yaml") as f:
        assert f.read() == "---\nfoo: bar\n"

    with file_utils.open(f"{TEST_DATA_PATH}/test.yaml", "rb") as f:
        assert f.read() == b"---\nfoo: bar\n"

    with pytest.raises(ValueError):
        file_utils.open(f"{TEST_DATA_PATH}/test.yaml", "w")

    with file_utils.open("$FILE_UTILS_TEST_DATA/test.yaml") as f:
        assert f.read() == "---\nfoo: bar\n"


def test_open_s3():
    with file_utils.open(f"{S3_TEST_DATA_PATH}/test.yaml") as f:
        assert f.read() == "---\nfoo: bar\n"

    with file_utils.open(f"{S3_TEST_DATA_PATH}/test.yaml", "rb") as f:
        assert f.read() == b"---\nfoo: bar\n"

    with pytest.raises(ValueError):
        file_utils.open(f"{S3_TEST_DATA_PATH}/test.yaml", "w")

    with file_utils.open("$S3_FILE_UTILS_TEST_DATA/test.yaml") as f:
        assert f.read() == "---\nfoo: bar\n"


def test_read_fits_filesystem():
    hdul = file_utils.read_fits(f"{TEST_DATA_PATH}/test.fits")
    assert hdul[0].header["FOO"] == "bar"

    hdul = file_utils.read_fits("$FILE_UTILS_TEST_DATA/test.fits")
    assert hdul[0].header["FOO"] == "bar"


def test_read_fits_s3():
    hdul = file_utils.read_fits(f"{S3_TEST_DATA_PATH}/test.fits")
    assert hdul[0].header["FOO"] == "bar"

    hdul = file_utils.read_fits("$S3_FILE_UTILS_TEST_DATA/test.fits")
    assert hdul[0].header["FOO"] == "bar"


def test_read_asdf_filesystem():
    with file_utils.read_asdf(f"{TEST_DATA_PATH}/test.asdf") as af:
        assert af.tree["foo"] == "bar"

    with file_utils.read_asdf("$FILE_UTILS_TEST_DATA/test.asdf") as af:
        assert af.tree["foo"] == "bar"


def test_read_asdf_s3():
    with file_utils.read_asdf(f"{S3_TEST_DATA_PATH}/test.asdf") as af:
        assert af.tree["foo"] == "bar"

    with file_utils.read_asdf("$S3_FILE_UTILS_TEST_DATA/test.asdf") as af:
        assert af.tree["foo"] == "bar"


def test_read_ascii_table_filesystem():
    table = file_utils.read_ascii_table(f"{TEST_DATA_PATH}/test.ascii")
    assert table["foo"][0] == "bar"

    table = file_utils.read_ascii_table("$FILE_UTILS_TEST_DATA/test.ascii")
    assert table["foo"][0] == "bar"


def test_read_ascii_table_s3():
    table = file_utils.read_ascii_table(f"{S3_TEST_DATA_PATH}/test.ascii")
    assert table["foo"][0] == "bar"

    table = file_utils.read_ascii_table("$S3_FILE_UTILS_TEST_DATA/test.ascii")
    assert table["foo"][0] == "bar"


def test_read_hdf5_filesystem():
    with file_utils.read_hdf5(f"{TEST_DATA_PATH}/test.hdf5") as f:
        assert f.attrs["foo"] == "bar"

    with file_utils.read_hdf5("$FILE_UTILS_TEST_DATA/test.hdf5") as f:
        assert f.attrs["foo"] == "bar"


def test_read_hdf5_s3():
    with file_utils.read_hdf5(f"{S3_TEST_DATA_PATH}/test.hdf5") as f:
        assert f.attrs["foo"] == "bar"

    with file_utils.read_hdf5("$S3_FILE_UTILS_TEST_DATA/test.hdf5") as f:
        assert f.attrs["foo"] == "bar"


def test_read_yaml_filesystem():
    result = file_utils.read_yaml(f"{TEST_DATA_PATH}/test.yaml")
    assert result["foo"] == "bar"

    result = file_utils.read_yaml("$FILE_UTILS_TEST_DATA/test.yaml")
    assert result["foo"] == "bar"


def test_read_yaml_s3():
    result = file_utils.read_yaml(f"{S3_TEST_DATA_PATH}/test.yaml")
    assert result["foo"] == "bar"

    result = file_utils.read_yaml("$S3_FILE_UTILS_TEST_DATA/test.yaml")
    assert result["foo"] == "bar"


def test_isfile_filesystem():
    assert file_utils.isfile(f"{TEST_DATA_PATH}/test.yaml") is True
    assert file_utils.isfile(f"{TEST_DATA_PATH}/blorp.yaml") is False

    assert file_utils.isfile(f"$FILE_UTILS_TEST_DATA/test.yaml") is True
    assert file_utils.isfile(f"$FILE_UTILS_TEST_DATA/blorp.yaml") is False


def test_isfile_s3():
    assert file_utils.isfile(f"{S3_TEST_DATA_PATH}/test.yaml") is True
    assert file_utils.isfile(f"{S3_TEST_DATA_PATH}/blorp.yaml") is False

    assert file_utils.isfile("$S3_FILE_UTILS_TEST_DATA/test.yaml") is True
    assert file_utils.isfile("$S3_FILE_UTILS_TEST_DATA/blorp.yaml") is False


def test_isdir_filesystem():
    assert file_utils.isdir(TEST_DATA_PATH) is True
    assert file_utils.isdir(f"{TEST_DATA_PATH}/foo") is False
    assert file_utils.isdir(f"{TEST_DATA_PATH}/test.yaml") is False

    assert file_utils.isdir("$FILE_UTILS_TEST_DATA") is True
    assert file_utils.isdir("$FILE_UTILS_TEST_DATA/foo") is False
    assert file_utils.isdir("$FILE_UTILS_TEST_DATA/test.yaml") is False


def test_isdir_s3():
    assert file_utils.isdir(S3_TEST_DATA_PATH) is True
    assert file_utils.isdir(f"{S3_TEST_DATA_PATH}/foo") is False
    assert file_utils.isdir(f"{S3_TEST_DATA_PATH}/test.yaml") is False

    assert file_utils.isdir("$S3_FILE_UTILS_TEST_DATA") is True
    assert file_utils.isdir("$S3_FILE_UTILS_TEST_DATA/foo") is False
    assert file_utils.isdir("$S3_FILE_UTILS_TEST_DATA/test.yaml") is False


def test_exists_filesystem():
    assert file_utils.exists(TEST_DATA_PATH) is True
    assert file_utils.exists(f"{TEST_DATA_PATH}/foo") is False
    assert file_utils.exists(f"{TEST_DATA_PATH}/test.yaml") is True

    assert file_utils.exists("$FILE_UTILS_TEST_DATA") is True
    assert file_utils.exists("$FILE_UTILS_TEST_DATA/foo") is False
    assert file_utils.exists("$FILE_UTILS_TEST_DATA/test.yaml") is True


def tests_exists_s3():
    assert file_utils.exists(S3_TEST_DATA_PATH) is True
    assert file_utils.exists(f"{S3_TEST_DATA_PATH}/foo") is False
    assert file_utils.exists(f"{S3_TEST_DATA_PATH}/test.yaml") is True

    assert file_utils.exists("$S3_FILE_UTILS_TEST_DATA") is True
    assert file_utils.exists("$S3_FILE_UTILS_TEST_DATA/foo") is False
    assert file_utils.exists("$S3_FILE_UTILS_TEST_DATA/test.yaml") is True


def test_abspath_filesystem():
    expected_path = f"{TEST_DATA_PATH}/test.yaml"
    assert file_utils.abspath(expected_path) == expected_path
    assert file_utils.abspath("$FILE_UTILS_TEST_DATA/test.yaml") == expected_path
    assert file_utils.abspath("tests/test_data/file_utils/test.yaml") == expected_path


def test_abspath_s3():
    expected_path = f"{S3_TEST_DATA_PATH}/test.yaml"
    assert file_utils.abspath(expected_path) == expected_path
    assert file_utils.abspath("$S3_FILE_UTILS_TEST_DATA/test.yaml") == expected_path


def test_glob_filesystem():
    result = file_utils.glob(f"{TEST_DATA_PATH}/*.yaml")
    assert result == [f"{TEST_DATA_PATH}/test.yaml"]
    result = file_utils.glob(f"{TEST_DATA_PATH}/*/*.yaml")
    assert result == [f"{TEST_DATA_PATH}/nested_dir/nested_test.yaml"]

    result = file_utils.glob("$FILE_UTILS_TEST_DATA/*.yaml")
    assert result == [f"{TEST_DATA_PATH}/test.yaml"]
    result = file_utils.glob("$FILE_UTILS_TEST_DATA/*/*.yaml")
    assert result == [f"{TEST_DATA_PATH}/nested_dir/nested_test.yaml"]


def test_glob_s3():
    result = file_utils.glob(f"{S3_TEST_DATA_PATH}/*.yaml")
    assert result == [f"{S3_TEST_DATA_PATH}/test.yaml"]
    result = file_utils.glob(f"{S3_TEST_DATA_PATH}/*/*.yaml")
    assert result == [f"{S3_TEST_DATA_PATH}/nested_dir/nested_test.yaml"]

    result = file_utils.glob("$S3_FILE_UTILS_TEST_DATA/*.yaml")
    assert result == [f"{S3_TEST_DATA_PATH}/test.yaml"]
    result = file_utils.glob("$S3_FILE_UTILS_TEST_DATA/*/*.yaml")
    assert result == [f"{S3_TEST_DATA_PATH}/nested_dir/nested_test.yaml"]
