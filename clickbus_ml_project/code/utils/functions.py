import logging
import sys
from sys import stdout
from datetime import datetime, timedelta
import os
from unicodedata import normalize
import re
import time
import random
import string
from io import StringIO
import boto3
import pandas as pd
import numpy as np

def config_log(flag_stdout=True, flag_logfile=False):
  """
  Applies log settings and returns a logging object.
  :flag_stdout: boolean
  :flag_logfile: boolean
  """
  handler_list = list()
  LOGGER = logging.getLogger()

  [LOGGER.removeHandler(h) for h in LOGGER.handlers]

  if flag_logfile:
      path_log = './logs/{}_{:%Y%m%d}.log'.format('log', datetime.now())
      if not os.path.isdir('./logs'):
          os.makedirs('./logs')
      handler_list.append(logging.FileHandler(path_log))

  if flag_stdout:
      handler_list.append(logging.StreamHandler(stdout))
      logging.basicConfig(
      level=logging.INFO\
      #,format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'\ format log antigo
      ,format='[%(asctime)s] %(levelname)s - %(message)s'\
      ,handlers=handler_list)
  return LOGGER

log = config_log()
def execute_query(query, connection):
    try:
        cursor = connection.cursor()
        result = cursor.execute(query)
        connection.commit()
    except Exception as e:
        log.error(e)
        raise(e)
