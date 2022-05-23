import logging
import os

""" import click """

from wd_extractor import WdExtractor

def run():
    WdExtractor().read_dump()

if __name__ == "__main__":
    run()
