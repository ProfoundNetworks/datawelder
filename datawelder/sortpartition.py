import sys
import datawelder.partition


def main():
    path = sys.argv[1]
    datawelder.partition.sort_partition(path, 0)


if __name__ == '__main__':
    main()
