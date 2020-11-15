#!/usr/bin/python2
# This is the script to run the non-batch version of VSIM
import subprocess
import time
import re
import os

######### Configurations ##########
### Data files (HDFS paths) ###
bn_human_raw_input_path = "wangshen/bn-human/bn-human-Jung2015_M87113878.edges"
bn_human_adjfile_path = "wangshen/bn-human/giraphInput"
bn_human_resort_path = "wangshen/bn-human/resort"
bn_human_adjdeg_path = "wangshen/bn-human/adjWithDeg"
pp_miner_raw_input_path = "wangshen/PP-Miner/PP-Miner_miner-ppi.tsv"
pp_miner_adjfile_path = "wangshen/PP-Miner/giraphInput"
pp_miner_resort_path = "wangshen/PP-Miner/resort"
pp_miner_adjdeg_path = "wangshen/PP-Miner/adjWithDeg"
com_livejournal_adjfile_path = "wangshen/livejournal/giraphInput"
com_livejournal_adjdeg_path = "wangshen/livejournal/adjWithDeg"
com_orkut_adjfile_path = "wangshen/orkut/giraphInput"
com_orkut_adjdeg_path = "wangshen/orkut/adjWithDeg"
uk_2002_raw_path = "/data/experiment/wzk/GraphDataSet/uk-2002/uk-2002" # local file
uk_2002_adjfile_path = "wangshen/uk-2002/giraphInput" #hdfs file
uk_2002_adjdeg_path = "wangshen/uk-2002/adjWithDeg"
friendster_adjfile_path = "wangshen/FriendSter/giraphInput"
friendster_adjdeg_path = "wangshen/FriendSter/adjWithDeg"

rmat_a30_raw_input_path = "/wzk/RMat/edgelist/V500kE100MA0.30"
rmat_a30_adjfile_path = "/wzk/RMat/adj/V500kE100MA0.30"
rmat_a30_resort_path = "/wzk/RMat/resort/V500kE100MA0.30"
rmat_a70_raw_input_path = "/wzk/RMat/edgelist/V500kE100MA0.70"
rmat_a70_adjfile_path = "/wzk/RMat/adj/V500kE100MA0.70"
rmat_a70_resort_path = "/wzk/RMat/resort/V500kE100MA0.70"

### Execution parameters ###
timeout = "3h"
jar_file = "vsim-5.0.jar"
machine_file = "machines"
network_interfaces = ["em1", "em2"]
task_file_name = 'task_input'
random_partition_file_name = 'random_partition_plan'

### Algorithm parameters ###
num_node = 16
num_core_per_node = 12
num_partition_run = num_node * 8
thread_num_per_executor = num_core_per_node * 4
db_write_batch_size = 2000
cache_capacity_in_byte = 25 * 1024 * 1024 * 1024
db_read_batch_size = 20
alpha = 1.2e-4
beta = 0.63
gamma = 3.25e-6
gamma_prime = 4.24e-5
execution_mode = "adaptive"
# execution_mode = "probe-count"
# execution_mode = "verification"

### Cache configuration
graph_store_conf_file_content_template="""
graphstore.db.backend.class.name=cn.edu.nju.pasalab.db.cache.CachedClient
cache.stats.file.path=/tmp/cache.stats.vsim
cache.concurrency=128
cache.hashtable.size.per.segment=16384
#cache.db.backend.class.name=cn.edu.nju.pasalab.db.hbase.HBaseClient
#cache.db.backend.class.name=cn.edu.nju.pasalab.graph.kvstore.KVStoreClientStandardWrapper
cache.db.backend.class.name=cn.edu.nju.pasalab.db.cassandra.CassandraClient
cache.capacity.in.byte={cache_capacity}
hbase.zookeeper.quorum=slave001,slave002,slave003
hbase.num.region=64
hbase.use.hashed.key=true
hbase.num.connection=4
cassandra.contact.points=192.168.100.12
mykvstore.hostlist=slave002:8999,slave003:8999,slave004:8999,slave005:8999,slave007:8999,slave008:8999,slave009:8999,slave010:8999,slave011:8999,slave012:8999,slave013:8999,slave014:8999,slave015:8999,slave016:8999,slave017:8999,slave018:8999
"""

def get_total_network_usage(machine_file, network_interfaces):
    """
    Get the sum of the current network usage of all machines and network interfaces.

    Args:
        machine_file: A string of the path of the machine file.
                      The machine file stores the hostnames/IPs of the
                      machines, each line a machine.
        network_interfaces: A list of strings containing the names of
                      the network interfaces.

    Returns:
        An integer of the sum of the current network usage of all machines and interfaces.
    """
    print("Checking network usage...")
    mf = open(machine_file, "r")
    total_bytes = 0L
    for machine in mf.readlines():
        if not machine.startswith("#"):
            for iface in network_interfaces:
                command = "ssh {machine} cat /sys/class/net/{iface}/statistics/tx_bytes".format(machine=machine.strip(), iface=iface)
                bytes_str = subprocess.check_output(command, shell=True)
                total_bytes = total_bytes + int(bytes_str)
    mf.close()
    print("Done.")
    return total_bytes

def run_command_with_timeout(command_line, timeout, stdout, stderr):
    """
    Runs a command in shell with a timeout.
    The command will be killed when exceeding the limit set by `timeout`.

    Args:
        command_line: A string of the command to run.
        timeout: A string describing the timeout. See the format of the /usr/bin/timeout program.
        stdout: A file object to write the command's stdout to.
        stderr: A file object to write the command's stderr to.

    Returns:
        An integer of the exit code of the command.
    """
    command_line_tout = "/usr/bin/timeout -k 10s -s 9 {tout} {cline}".format(tout=timeout, cline=command_line)
    exit_code = subprocess.call(command_line_tout, stdout=stdout, stderr=stderr, shell=True)
    return exit_code

def prepare_log_file(run_mode, dataset):
    """
    Prepares log files for an execution of a given algorithm and a dataset.
    This function generates two log files for stdout and stderr respectively.
    The file names are `logs/{alg_name}/{dataset}-timestamp.stdout` and
    `logs/{alg_name}/{dataset}-timestamp.stderr` respectively.

    Args:
        alg_name: A string of the name of the algorithm.
        dataset: A string of the name of the dataset.

    Returns:
        A pair of two file objects (stdout, stderr).
        The file objects should be closed by the user.
    """
    time_str = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
    subprocess.call("mkdir -p logs/{mode}".format(mode=run_mode), shell=True)
    stdout_f = open("logs/{mode}/{ds}-{tmstamp}.stdout".format(mode=run_mode, ds=dataset, tmstamp=time_str), "w", 0)
    stderr_f = open("logs/{mode}/{ds}-{tmstamp}.stderr".format(mode=run_mode, ds=dataset, tmstamp=time_str), "w", 0)
    return (stdout_f, stderr_f)

def kill_yarn_running_jobs():
    yarn_output = subprocess.check_output("yarn application -list", shell=True)
    application_ids = re.findall("application_[0-9]+_[0-9]+\t", yarn_output)
    for application_id in application_ids:
        subprocess.call("yarn application -kill {id}".format(id=application_id), shell=True)

def prepare_graph_store_conf_file(cache_capacity):
    f = open("pasa.conf.prop", "w")
    content = graph_store_conf_file_content_template.format(cache_capacity=cache_capacity)
    f.write(content)
    f.close()

def prepare_random_partition_file(dataset, input_path):
    """
    Prepare the random partition file for the dataset
    """
    data_dir, file_name = os.path.split(input_path)
    plan_file_path = os.path.join(data_dir, "{base}_{numPart}".format(base=random_partition_file_name, numPart=num_partition_run))
    out_f, err_f = prepare_log_file("vsim-preprocess", dataset)
    print("Run test case: VSIM-preprocess {}, generate random partition file, {}".format(dataset, str(out_f)))
    log_file = open("vsim-preprocess.log", "a", 0)
    log_file.write("Run test case: VSIM-Preprocess {}, generate random partition file, {}\n".format(dataset, str(out_f)))
    kill_yarn_running_jobs()
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_file.write("Start time: {}\n".format(time_str))
    # store the data into the database
    command_line = "spark-submit --master yarn --deploy-mode client \
    --executor-cores {cores} --executor-memory 20g --num-executors {nodes} --driver-memory 20g \
    --class cn.edu.nju.pasalab.graph.partitioning.GenerateRandomPartitionPlanFile \
    --conf spark.task.maxFailures=1  \
     {jar} {input_path} {num_partition} {partition_plan_file}".format(num_partition=num_partition_run,
                                                            nodes=num_node, cores=num_core_per_node * 2,
                                                            jar=jar_file, input_path=input_path,
                                                            partition_plan_file=plan_file_path)
    exit_code = run_command_with_timeout(command_line, timeout, out_f, err_f)

def run_preprocess(dataset, input_path):
    """
    Run preprocessing of VSIM.

    Args:
        dataset: A string of the name of the dataset.
        input_path: A string of the HDFS path of the input file.

    Returns:
        An integer representing the exit code of the spark program.

    """
    data_dir, file_name = os.path.split(input_path)
    partition_plan_path = os.path.join(data_dir, "{}_{}".format(random_partition_file_name, num_partition_run))
    task_path = os.path.join(data_dir, task_file_name)
    out_f, err_f = prepare_log_file("vsim-preprocess", dataset)
    print("Run test case: VSIM-preprocess {}, {}".format(dataset, str(out_f)))
    log_file = open("vsim-preprocess.log", "a", 0)
    log_file.write("Run test case: VSIM-Preprocess {}, {}\n".format(dataset, str(out_f)))
    kill_yarn_running_jobs()
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_file.write("Start time: {}\n".format(time_str))
    # store the data into the database
    command_line = "spark-submit --master yarn --deploy-mode client \
    --executor-cores {cores} --executor-memory 20g --num-executors {nodes} --driver-memory 20g \
    --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true \
    --class cn.edu.nju.pasalab.graph.vsim.Preprocess \
    --conf spark.task.maxFailures=1 --conf spark.locality.wait=0s --files pasa.conf.prop \
     {jar} {input_path} {num_partition} {batch_size} {task_path} {parallelism} {partition_path}".format(num_partition=num_partition_run,
                                                            nodes=num_node, cores=num_core_per_node * 2,
                                                            jar=jar_file, input_path=input_path,
                                                            batch_size=db_write_batch_size, task_path=task_path,
                                                            parallelism=num_node * num_core_per_node * 2,
                                                            partition_path=partition_plan_path)
    prepare_graph_store_conf_file(cache_capacity_in_byte)
    network_before = get_total_network_usage(machine_file, network_interfaces)
    start_time = time.time()
    exit_code = run_command_with_timeout(command_line, timeout, out_f, err_f)
    end_time = time.time()
    network_after = get_total_network_usage(machine_file, network_interfaces)
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_file.write("End time: {}.\n".format(time_str))
    elapsed_time = end_time - start_time
    network_usage = network_after - network_before
    log_file.write("Exit code: {}.\n".format(exit_code))
    log_file.write("Elapsed time: {} s.\n".format(elapsed_time))
    log_file.write("Network usage: {} bytes.\n".format(network_usage))
    out_f.close()
    err_f.close()
    log_file.close()
    return exit_code

def run_calc(dataset, input_path, threshold):
    """
    Runs calculation of VSIM with a test case.

    Args:
        dataset: A string of the name of the dataset.
        input_path: A string of the HDFS path of the input file.
        threshold: similarity threshold

    Returns:
        An integer representing the exit code of the spark program.

    """
    data_dir, file_name = os.path.split(input_path)
    task_path = os.path.join(data_dir, task_file_name)
    out_f, err_f = prepare_log_file("vsim-run", dataset)
    print("Run test case: VSIM {}, {}, {}".format(dataset, str(out_f), threshold))
    log_file = open("vsim-run-{}.log".format(execution_mode), "a", 0)
    log_file.write("Run test case: VSIM {}, {}, {}\n".format(dataset, str(out_f), threshold))
    kill_yarn_running_jobs()
    run_command_with_timeout("bash ./clear_cache_stats.sh", timeout, out_f, err_f)
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_file.write("Start time: {}\n".format(time_str))
    command_line = "spark-submit --master yarn --deploy-mode client \
    --executor-cores 12 --executor-memory 12g --num-executors {nodes} --driver-memory 20g \
    --class cn.edu.nju.pasalab.graph.vsim.VSIM \
    --conf spark.executor.memoryOverhead=35g  --conf spark.yarn.executor.memoryOverhead=35g \
    --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true \
    --conf spark.task.maxFailures=1 --conf spark.locality.wait=0s --files pasa.conf.prop \
     {jar} {input_path} {num_partition} {mode} {threshold} {thread_num_per_executor} {output_stat} {output_path} {batch} {task_path} \
     {alpha} {beta} {gamma} {gamma_prime}".format(
                                                                   nodes=num_node, jar=jar_file, input_path=input_path,
                                                                   threshold=threshold, num_partition=num_partition_run,
                                                                   mode=execution_mode, thread_num_per_executor=thread_num_per_executor,
                                                                   output_stat="false", output_path="null",
                                                                   batch=db_read_batch_size, task_path=task_path,
                                                                   alpha=alpha, beta=beta, gamma=gamma, gamma_prime=gamma_prime)
    prepare_graph_store_conf_file(cache_capacity_in_byte)
    network_before = get_total_network_usage(machine_file, network_interfaces)
    start_time = time.time()
    exit_code = run_command_with_timeout(command_line, timeout, out_f, err_f)
    end_time = time.time()
    network_after = get_total_network_usage(machine_file, network_interfaces)
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_file.write("End time: {}.\n".format(time_str))
    elapsed_time = end_time - start_time
    network_usage = network_after - network_before
    log_file.write("Exit code: {}.\n".format(exit_code))
    log_file.write("Elapsed time: {} s.\n".format(elapsed_time))
    log_file.write("Network usage: {} bytes.\n".format(network_usage))
    run_command_with_timeout("bash ./collect_cache_stats.sh", timeout, out_f, err_f)
    out_f.close()
    err_f.close()
    log_file.close()
    return exit_code

def run_n_times(func, times):
    """Runs a function for n times.

    Runs a given function for n times.
    The function will be called without any argument.
    The function should return an integer as the return value.
    If the return value is not zero for an execution, the further executions will be prevented.

    Args:
        func: A function to run.
        times: An integer representing the running times of the function.
    """
    for t in range(0, times):
        print("time: {}".format(t))
        exit_code = func()
        if exit_code != 0:
            print("exit code: {}".format(exit_code))
            print("break.")
            break


dataset_files = {"com-orkut": com_orkut_adjdeg_path,
        "bn-human": bn_human_adjdeg_path,
        "pp-miner": pp_miner_adjdeg_path,
        "Friendster": friendster_adjdeg_path,
        "uk-2002": uk_2002_adjdeg_path}

for dataset in ["Friendster"]:
    #prepare_random_partition_file(dataset, dataset_files[dataset])
    run_preprocess(dataset, dataset_files[dataset])
    #for threshold in [1.0, 0.9, 0.8]:
    for threshold in [1.0, 0.9, .8, .7, .6, .5, .4, .3, .2, .1, 2e-6]:
        run_calc(dataset, dataset_files[dataset], threshold - 1e-6)
