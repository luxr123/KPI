import os,sys

def get_conf(path, split):
    d = {}
    with open(path) as fin:
        for line in fin:
            items = line.strip().split(split)
            if len(items) != 2:
                continue
            d[items[0].strip()] = items[1].strip()
    return d


if __name__ == '__main__':
	print(get_conf('../conf/hadoop.conf', '='))
