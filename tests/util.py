""" Test helper functions. """

from pathlib import Path
from tests.constants import TEST_DIR

TEST_DATABASE_DIR = Path(__file__).parent / "test_databases"


def _destroy_database():
    """ Deletes sqlite database files from test_databases directory. """
    for path in TEST_DATABASE_DIR.iterdir():
        if path.is_file():
            path_tokens = str(path).split("/")
            file_name = path_tokens[-1]
            if ".sqlite" in file_name:
                path.unlink()


def tear_down():
    """ Tear down fixture that will remove database and log files. """
    _destroy_test_logs(TEST_DIR)
    _destroy_database()


def _destroy_test_logs(root_dir: Path) -> None:
    """ Recursive function to delete logs from test directory.

    Params:
        root_dir: Path to root directory to have logs deleted.
    """
    delete_dirs = ['logs']
    for path in root_dir.iterdir():
        if path.is_dir():
            path_tokens = str(path).split("/")
            dir_name = path_tokens[-1]
            # ignoring nontest dirs
            if "__" not in dir_name:
                if dir_name in delete_dirs:
                    _remove_dir_tree(path)
                else:
                    _destroy_test_logs(path)


def _remove_dir_tree(root_dir_path: Path):
    """ Recursively deletes a directory tree from root.

    Params:
        root_dir_path: Path to root directory to be recursively deleted.
    """
    for path in root_dir_path.iterdir():
        if path.is_dir():
            _remove_dir_tree(path)
        else:
            path.unlink()
    root_dir_path.rmdir()
