from __future__ import print_function

from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic, register_cell_magic,
                                register_line_magic, register_line_cell_magic, needs_local_scope)
from IPython.core.display import display
import argparse
import pandas as pd
from pyalink.alink import *


@magics_class
class AlinkMagics(Magics):
    def __init__(self, shell):
        super(AlinkMagics, self).__init__(shell)
        self.shell = shell
        alink_parser = argparse.ArgumentParser(prog="%%alink")
        alink_parser.add_argument("--flink_home", help="--flinkHome path_to_local_flink_home")
        alink_parser.add_argument("--conf", action="append", default=[], help="e.g., --conf parallelism=1")
        self.alink_argparser = alink_parser

    @line_cell_magic
    def alink(self, line, cell=None):
        if cell is None:
            options = self.alink_argparser.parse_args(line.split())
        else:
            options = self.alink_argparser.parse_args((line + ' ' + cell).split())
        flink_home = None
        conf = {}
        if options.flink_home:
            flink_home = options.flink_home
        for kv in options.conf:
            k, v = kv.split('=', 1)
            conf[k] = v
        parallelism = 1
        if "parallelism" in conf.keys():
            parallelism = int(conf["parallelism"])

        # resetEnv()
        env, btenv, senv, stenv = useLocalEnv(parallelism=parallelism, flinkHome=flink_home, config=None)
        # env, btenv, senv, stenv = getMLEnv()

        self.shell.user_ns.update({"env": env, "btenv": btenv, "senv": senv, "stenv": stenv})
        data = ["env", "btenv", "senv", "stenv"]
        df = pd.DataFrame(index=["ExecutionEnvironment", "BatchTableEnvironment",
                                 "StreamExecutionEnvironment", "StreamTableEnvironment"],
                          data=data, columns=["变量名"])
        display(df)
