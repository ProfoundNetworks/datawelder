import pickle
import sys

import smart_open


def main():
    with smart_open.open(sys.argv[1], 'rb') as fin:
        while True:
            try:
                print(pickle.load(fin))
            except EOFError:
                break


if __name__ == '__main__':
    main()
