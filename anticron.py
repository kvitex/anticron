#!/usr/bin/env python3
from prometheus_client import start_http_server, Gauge
from pprint import pprint
from datetime import datetime
import yaml
import re
import sys
import subprocess
import time
import schedule

class Stask(object):
    def __init__(self, **kwargs):
        self.time_at = kwargs['time_at']
        self.command = kwargs['command']
        self.name = kwargs['name']
        self.last_exit_code = 0
        self.last_task_result = 0
        self.task_status = 0 # 0-idle, 1-runnig, 2-stucked
        self.last_success_timestamp = 0
        self.last_task_duration = 0
        self.last_task_start_timestamp = 0
        self.process = None

    def run(self):
        self.poll()
        print('{} Starting task {}:'.format(datetime.now(), self.name))
        if self.process is None: 
            self.process = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE)
            self.task_status = 1
            self.last_task_start_timestamp = time.time()
        else:
            proc_code = self.process.poll()
            if proc_code is None:
                self.task_status = 2
                return
            else:
                self.process = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.task_status = 1
                self.last_task_start_timestamp = time.time()

    def poll(self):
        if self.process is None:
            return None
        else:
            proc_code = self.process.poll()
            if proc_code is None:
                return None
            else:
                std_out = ''
                std_err = ''
                if self.process.stdout is not None:
                    std_out = self.process.stdout.read().decode('utf-8').replace('\\n','\n')
                if self.process.stderr is not None:
                    std_err = self.process.stderr.read().decode('utf-8').replace('\\n','\n')
                any(map(lambda x: print('{}: {}'. format(self.name, x)), std_err.split('\n')))
                any(map(lambda x: print('{}: {}'. format(self.name, x)), std_out.split('\n')))
                if proc_code == 0:
                    self.last_task_result = 1
                    self.last_success_timestamp = time.time()
                else:
                    self.last_task_result = 0
                self.last_exit_code = proc_code
                self.last_task_duration = time.time() - self.last_task_start_timestamp
                self.task_status = 0
                self.process = None
                print('{} Task stopped {}: with exit code: {}'.format(datetime.now(), self.name, proc_code))
                return proc_code


if __name__ == "__main__":
    startTime = time.time()
    if len(sys.argv) < 2:
        print('Usage: anticron <config file>')
        exit(255)
    else:
        config_file_name = sys.argv[1]
    try:
        with open(config_file_name) as config_file:
            cfg = yaml.load(config_file.read())
    except FileNotFoundError or FileExistsError as Error:
        print('Can not open configuration file {}'.format(config_file_name))
        print(Error)
        exit(-1)
    except yaml.scanner.ScannerError as Error:
        print('Error while parsing configuration file {}'.format(config_file_name))
        print(Error)
        exit(-1)
    except Exception as Error:
        print(Error)
        exit(-1)
    start_http_server(cfg['http_port'])
    ac_last_exit_code = Gauge(
        'ac_last_exit_code',
        'Last exit code for task',
        ['name']
        ) 
    ac_task_status = Gauge(
        'ac_task_status',
        'Current task status',
        ['name']
        ) 
    ac_last_task_result = Gauge(
        'ac_last_task_result',
        'Last task execution result',
        ['name']
        ) 
    ac_last_success_timestamp = Gauge(
        'ac_last_success_timestamp',
        'Last success  result timestamp ',
        ['name']
        ) 
    ac_last_task_duration = Gauge(
        'ac_last_task_duration',
        'Last task execution duration',
        ['name']
        ) 
    ac_last_task_start_timestamp = Gauge(
        'ac_last_task_start_timestamp',
        'Last task start timestamp',
        ['name']
        )
    tasks_dict = {}
    for taskcfg in cfg['tasks']:
        tasks_dict[taskcfg['name']] = Stask(**taskcfg)
        print(tasks_dict[taskcfg['name']])
        schedule.every().day.at(taskcfg['time_at']).do(tasks_dict[taskcfg['name']].run)
    while True:
        for name in tasks_dict:
            tasks_dict[name].poll()
        for taskcfg in cfg['tasks']:
            ac_labels = {
                'name': taskcfg['name']
            }
            ac_last_exit_code.labels(**ac_labels).set(tasks_dict[taskcfg['name']].last_exit_code) 
            ac_task_status.labels(**ac_labels).set(tasks_dict[taskcfg['name']].task_status)
            ac_last_task_result.labels(**ac_labels).set(tasks_dict[taskcfg['name']].last_task_result)    
            ac_last_success_timestamp.labels(**ac_labels).set(tasks_dict[taskcfg['name']].last_success_timestamp)
            ac_last_task_duration.labels(**ac_labels).set(tasks_dict[taskcfg['name']].last_task_duration)
            ac_last_task_start_timestamp.labels(**ac_labels).set(tasks_dict[taskcfg['name']].last_task_start_timestamp)
        schedule.run_pending()
        time.sleep(cfg.get('rtimer', 60))