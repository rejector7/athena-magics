from __future__ import print_function

from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic, register_cell_magic,
                                register_line_magic, register_line_cell_magic, needs_local_scope)
from IPython.core.display import display
import argparse
import pandas as pd
from pyflink.table import *


@magics_class
class FlinkMagics(Magics):

    def __init__(self, shell):
        super(FlinkMagics, self).__init__(shell)

        flink_parser = argparse.ArgumentParser(prog="%%flink")
        flink_parser.add_argument("--env_name")
        flink_parser.add_argument("--conf", action="append", default=[], help="e.g., --conf parallelism.default=1")
        self.flink_argparser = flink_parser

        # sql_parser = argparse.ArgumentParser(prog="%%sql")
        # sql_parser.add_argument('--preview', action='store_true')
        # sql_parser.add_argument('--quiet', action='store_true')
        # sql_parser.add_argument('--cache', action='store_true')
        # self.sql_argparser = sql_parser

    @line_cell_magic
    def flink(self, line, cell=None):
        environment_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
        t_env = BatchTableEnvironment.create(environment_settings=environment_settings)
        env_conf = t_env.get_config().get_configuration()
        env_conf.set_string("parallelism.default", "1")

        if cell is None:
            options = self.flink_argparser.parse_args(line.split())
        else:
            options = self.flink_argparser.parse_args((line + ' ' + cell).split())

        env_name = "t_env"
        if options.env_name:
            env_name = options.env_name
        for kv in options.conf:
            k, v = kv.split('=', 1)
            env_conf.set_string(k, v)

        self.shell.user_ns.update({env_name: t_env})
        data = [env_name]
        df = pd.DataFrame(index=["BatchTableEnvironment"], data=data, columns=["变量名"])
        display(df)
