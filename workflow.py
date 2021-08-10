#!/usr/bin/env python3
from Pegasus.api import *

import os
import sys
import logging
import subprocess
import yaml
from pathlib import Path
from datetime import datetime
from argparse import ArgumentParser
from typing import Optional, Tuple, Union, Dict, List

logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API ------------------------------------------------------


class IOSyntheticWorkflow(object):
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None
    decaf = None

    # --- Init ----------------------------------------------------------------
    def __init__(self,
                 wf_name: Optional[str] = 'io-synthetic',
                 shape: Optional[Tuple[str, Union[int, str]]] = ("chain", 1),
                 exec_site_name: Optional[str] = "condorpool",
                 binary_path: Optional[str] = "vanilla",
                 files_size: Optional[Union[List[float], Dict[str,float]]]=[1.0],
                 size_unit: Optional[str] = 'G',
                 waiting_time: Optional[Union[List[float], Dict[str,float]]] = [2.0]
                ) -> None:
        self.wf_name = wf_name
        self.wid = self.wf_name + "-" + datetime.now().strftime("%s")
        self.dagfile = self.wid+".yml"

        self.wf_dir = str(Path(__file__).parent.resolve())
        self.exec_site_name = exec_site_name

        self.shape = shape
        self.binary_path = binary_path
        self.size_unit = size_unit.upper()
        self.files_size = files_size
        self.waiting_time = waiting_time
        self.decaf = False
        if self.shape[0] == "decaf":
            self.decaf = True

        ## Security checks
        if self.size_unit not in ['B', 'K', 'M', 'G']:
            print("Error: Unit size accepted values are [B, K, M, G] (default: G).")
            sys.exit(-1)
        
        if isinstance(files_size, list) and len(files_size) == 1:
            if isinstance(self.shape[1], int):
                self.files_size = files_size*self.shape[1]
            elif isinstance(self.shape[1], str) and self.shape[0] != "custom":
                print("Error: Number of nodes must be an integer.")
                sys.exit(-1)

        if len(self.files_size) != self.shape[1]:
            print("Error: File size list lenght ({}) must be equal to the number of nodes ({}).".format(
                len(self.files_size), self.shape[1]))
            sys.exit(-1)

        if isinstance(waiting_time, list) and len(waiting_time) == 1:
            if isinstance(self.shape[1], int):
                self.waiting_time = waiting_time*self.shape[1]
            else:
                print("Error: Number of nodes must be an integer.")
                sys.exit(-1)
        
        if len(self.waiting_time) != self.shape[1]:
            print("Error: Waiting time list lenght ({}) must be equal to the number of nodes ({}).".format(
                len(self.waiting_time), self.shape[1]))
            sys.exit(-1)

        ## Output Sites
        self.shared_scratch_dir = os.path.join(
            self.wf_dir, "{}/scratch".format(self.wid))
        self.local_storage_dir = os.path.join(
            self.wf_dir, "{}/output".format(self.wid))

    # --- Write files in directory --------------------------------------------
    def write(self):
        if not self.sc is None:
            self.sc.write()
        self.props.write()
        self.tc.write()
        self.rc.write()
        self.wf.write()

    # --- Configuration (Pegasus Properties) ----------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()
        if self.decaf :
            self.props["pegasus.job.aggregator"] = "Decaf"
            self.props["pegasus.data.configuration"] = "sharedfs"
        # props["pegasus.monitord.encoding"] = "json"
        # self.properties["pegasus.integrity.checking"] = "none"
        
        return

    # --- Site Catalog --------------------------------------------------------
    def create_sites_catalog(self) -> None:
        self.sc = SiteCatalog()

        local = Site("local").add_directories(
            Directory(Directory.SHARED_SCRATCH, self.shared_scratch_dir).add_file_servers(
                FileServer("file://" + self.shared_scratch_dir, Operation.ALL)
            ),
            Directory(Directory.LOCAL_STORAGE, self.local_storage_dir).add_file_servers(
                FileServer("file://" + self.local_storage_dir, Operation.ALL)
            ),
        )

        condorpool = (
            Site("condorpool")
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        )

        self.sc.add_sites(local, condorpool)

        if self.exec_site_name == "cori":
            cori = (
                Site("cori")
                .add_grids(
                    Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.SLURM, contact="${NERSC_USER}@cori.nersc.gov", job_type=SupportedJobs.COMPUTE),
                    Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.SLURM, contact="${NERSC_USER}@cori.nersc.gov", job_type=SupportedJobs.AUXILLARY)
                )
                .add_directories(
                    Directory(Directory.SHARED_SCRATCH, "/global/cscratch1/sd/${NERSC_USER}/pegasus/scratch")
                        .add_file_servers(FileServer("file:///global/cscratch1/sd/${NERSC_USER}/pegasus/scratch", Operation.ALL)),
                    Directory(Directory.SHARED_STORAGE, "/global/cscratch1/sd/${NERSC_USER}/pegasus/storage")
                        .add_file_servers(FileServer("file:///global/cscratch1/sd/${NERSC_USER}/pegasus/storage", Operation.ALL))
                )
                .add_pegasus_profile(
                    style="ssh",
                    data_configuration="sharedfs",
                    change_dir="true",
                    project="${NERSC_PROJECT}",
                    runtime="300"
                    # grid_start = "NoGridStart"
                )
                .add_env(key="PEGASUS_HOME", value="${NERSC_PEGASUS_HOME}")
            )
            self.sc.add_sites(cori)


    # --- Transformation Catalog (Executables and Containers) -----------------
    def create_transformation_catalog(self) -> None:
        self.tc = TransformationCatalog()

        
        # --- Transformations ---------------------------------------------------------------
        try:
            pegasus_config = subprocess.run(
                ["pegasus-config", "--bin"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
        except FileNotFoundError as e:
            print("Unable to find pegasus-config")

        assert pegasus_config.returncode == 0

        PEGASUS_BIN_DIR = pegasus_config.stdout.decode().strip()
        keg = Transformation(
            "keg",
            site="local",
            pfn=PEGASUS_BIN_DIR + "/pegasus-keg",
            is_stageable=True
        )
        if self.binary_path:
            keg = Transformation(
                "keg",
                site="local",
                pfn=self.binary_path,
                is_stageable=True
            )
        
        if self.exec_site_name == "cori":
            pegasus_transfer = (
                Transformation("transfer", namespace="pegasus", site="cori", pfn="$PEGASUS_HOME/bin/pegasus-transfer", is_stageable=False)\
                .add_pegasus_profile(
                    queue="@escori",
                    runtime="300",
                    glite_arguments="--qos=xfer --licenses=SCRATCH"
                )
                .add_profiles(Namespace.PEGASUS, key="transfer.threads", value="8")
                .add_env(key="PEGASUS_TRANSFER_THREADS", value="8")    
            )
            pegasus_dirmanager = (
                Transformation("dirmanager", namespace="pegasus", site="cori", pfn="$PEGASUS_HOME/bin/pegasus-transfer", is_stageable=False)\
                .add_pegasus_profile(
                    queue="@escori",
                    runtime="300",
                    glite_arguments="--qos=xfer --licenses=SCRATCH"
                )
            )
            pegasus_cleanup = (
                Transformation("cleanup", namespace="pegasus", site="cori", pfn="$PEGASUS_HOME/bin/pegasus-transfer", is_stageable=False)
                .add_pegasus_profile(
                    queue="@escori",
                    runtime="60",
                    glite_arguments="--qos=xfer --licenses=SCRATCH"
                )
            )
            system_chmod = (
                Transformation("chmod", namespace="system", site="cori", pfn="/usr/bin/chmod", is_stageable=False)
                .add_pegasus_profile(
                    queue="@escori",
                    runtime="120",
                    glite_arguments="--qos=xfer --licenses=SCRATCH"
                )
            )

            github_location = "https://raw.githubusercontent.com/pegasus-isi/io-synthetic/master"
            wrapper_fn = os.path.join(github_location, "bin/wrapper_kickstart.sh")

            exec_path = "$PEGASUS_HOME/bin/pegasus-keg"
            if self.binary_path:
                exec_path = self.binary_path

            # keg = (
            #     Transformation("keg", site="cori", pfn=wrapper_fn, is_stageable=True)
            #     .add_pegasus_profile(
            #         cores="1",
            #         runtime="1800",
            #         glite_arguments="--qos=debug --constraint=haswell --licenses=SCRATCH",
            #     )
            #     .add_env(key="USER_HOME", value="${NERSC_USER_HOME}")
            #     .add_env(key="EXEC", value=exec_path)
            # )
            keg = (
                Transformation("keg", site="cori", pfn=exec_path, is_stageable=True)
                .add_pegasus_profile(
                    cores="1",
                    runtime="1800",
                    glite_arguments="--qos=regular --constraint=haswell --licenses=SCRATCH",
                )
            )
                       
            self.tc.add_transformations(pegasus_transfer, pegasus_dirmanager, pegasus_cleanup, system_chmod)
            
            # What is this tranformation for?
            if self.decaf:
                env_script="/global/common/software/m2187/pegasus-keg/decaf/env.sh"
                json_fn="linear2.json"
                decaf = (
                    Transformation("decaf", namespace="dataflow", site="cori", pfn=json_fn, is_stageable=False)
                    .add_pegasus_profile(
                        runtime="1800",
                        glite_arguments="--qos=debug --constraint=haswell --licenses=SCRATCH",
                    )
                    .add_env(key="DECAF_ENV_SOURCE", value=env_script)  
                )
                self.tc.add_transformations(decaf)

        self.tc.add_transformations(keg)


    # --- Replica Catalog -----------------    
    def create_replica_catalog(self, file_path) -> None:
        self.rc = ReplicaCatalog()

        file_site = "local"
        if self.exec_site_name == "cori":
            file_site = "cori"
        
        if file_path:
            self.rc.add_replica(file_site, "f0.txt", file_path)

    # --- Create Workflow -----------------------------------------------------
    def create_workflow(self) -> None:
        if self.shape[0] == "chain":
            self.create_workflow_chain()
        elif self.shape[0] == "new_chain":
            self.create_workflow_new_chain()
        elif self.shape[0] == "fork":
            self.create_workflow_fork()
        elif self.shape[0] == "decaf":
            self.create_workflow_decaf()
        else:
            self.create_workflow_custom()

    def create_workflow_chain(self) -> None:
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        ## First job
        f1 = File("j1.txt")
        job1 = (
            Job("keg")
            .add_args("-o", f1, "-s", self.waiting_time[0], "-G", self.files_size[0], "-u", self.size_unit)
            .add_outputs(f1, stage_out=False, register_replica=True)
        )
        self.wf.add_jobs(job1)

        # Security check to ensure nb of jobs is positive >= 1
        nb_jobs = max(self.shape[1], 1)

        for i in range(1, nb_jobs):
            fi = File("f{}.txt".format(i))
            keg = (
                Job("keg")
                .add_args("-i", f1, "-o", fi, "-s", self.waiting_time[i], "-G", self.files_size[i], "-u", self.size_unit)
                .add_inputs(f1)
                .add_outputs(fi, stage_out=False, register_replica=False)
            )
            f1 = fi
            self.wf.add_jobs(keg)

    def create_workflow_new_chain(self) -> None:
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        # Security check to ensure nb of jobs is positive >= 1
        nb_jobs = max(self.shape[1], 1)

        f1 = File("f0.txt")
        for i in range(1, nb_jobs+1):
            fi = File("f{}.txt".format(i))
            keg = (
                Job("keg")
                # .add_args("-i", f1, "-o", fi, "-s", self.waiting_time[i-1], "-G", self.files_size[i-1], "-u", self.size_unit)
                .add_args("-i", f1, "-o", fi, "-s", self.waiting_time[i-1])
                # .add_args("-i", f1, "-o", fi)
                .add_inputs(f1)
                .add_outputs(fi, stage_out=False, register_replica=False)
            )
            if self.decaf:
                keg.add_profiles(Namespace.PEGASUS, key="label", value="cluster1")
            f1 = fi
            self.wf.add_jobs(keg)

    def create_workflow_decaf(self) -> None:
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        # Security check to ensure nb of jobs is positive >= 1
        nb_jobs = max(self.shape[1], 1)
        
        # First job
        f1 = File("f0.txt")
        job1 = (
            Job("keg")
            .add_profiles(Namespace.PEGASUS, key="label", value="cluster1")
            .add_args("-i", f1, "-D", self.files_size[0], "-s", self.waiting_time[0])
            .add_inputs(f1)
            # .add_outputs(f1, stage_out=False, register_replica=True)
        )
        self.wf.add_jobs(job1)
        
        for i in range(1, nb_jobs-1):
            fi = File("f{}.txt".format(i))
            keg = (
                Job("keg")
                .add_profiles(Namespace.PEGASUS, key="label", value="cluster1")
                .add_args("-D", self.files_size[i], "-s", self.waiting_time[i])
            )
            self.wf.add_jobs(keg)

        # Last job
        fn = File("f{}.txt".format(nb_jobs-1))
        jobn = (
            Job("keg")
            .add_profiles(Namespace.PEGASUS, key="label", value="cluster1")
            .add_args("-o", fn, "-D", self.files_size[nb_jobs-1], "-s", self.waiting_time[nb_jobs-1])
            .add_outputs(fn, stage_out=False, register_replica=True)
        )
        self.wf.add_jobs(jobn)

    def create_workflow_fork(self) -> None:
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        ## First job
        f1 = File("j1.txt")
        job1 = (
            Job("keg")
            .add_args("-o", f1, "-s", self.waiting_time[0], "-G", self.files_size[0], "-u", self.size_unit)
            .add_outputs(f1, stage_out=False, register_replica=False)
        )
        self.wf.add_jobs(job1)

        # Security check to ensure nb of jobs is positive >= 1
        nb_jobs = max(self.shape[1], 1)

        flast = File("f{}.txt".format(nb_jobs))
        joblast = (
            Job("keg")
            .add_args("-s", self.waiting_time[nb_jobs-1])
            .add_outputs(flast, stage_out=False, register_replica=False)
        )

        for i in range(1, nb_jobs-1):
            fi = File("f{}.txt".format(i))
            keg = (
                Job("keg")
                .add_args("-i", f1, "-o", fi, "-s", self.waiting_time[i], "-G", self.files_size[i], "-u", self.size_unit)
                .add_inputs(f1)
                .add_outputs(fi, stage_out=False, register_replica=False)
            )

            joblast.add_args("-i", fi)
            joblast.add_inputs(fi)

            self.wf.add_jobs(keg)

        ## We add the last job
        self.wf.add_jobs(joblast)

    # --- Create Workflow -----------------------------------------------------
    def create_workflow_custom(self, 
            kickstart_record: Optional[Union[List[str], Dict[str, float]]] = None
        ) -> None:
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        file = self.shape[1]

        if kickstart_record is not None:
            raise NotImplementedError("kickstart_record support not yet implemented")

        with open(file, 'r') as f:
            try:
                data = yaml.safe_load(f)

                for job in data['jobs']:
                    keg = Job("keg")
                    keg.add_args("-G", 1, "-u", self.size_unit)

                    for lfn in job['uses']:
                        fi = File(lfn['lfn'])

                        if lfn['type'] == "input":
                            keg.add_inputs(fi)
                            keg.add_args("-i", fi)
                        else:
                            keg.add_outputs(fi, stage_out=lfn['stageOut'],
                                            register_replica=lfn['registerReplica'])
                            keg.add_args("-o", fi)
                    
                    self.wf.add_jobs(keg)

            except yaml.YAMLError as e:
                print(e)
                sys.exit(-1)


    # --- Run Workflow -----------------------------------------------------
    def run(self, submit=False, wait=False):
        try:
            plan_site = [self.exec_site_name]
            cluster_type = None
            if self.decaf: 
                cluster_type = ["label"]
            (
                self.wf.plan(
                dir=self.wf_dir,
                # relative_dir=self.wf_name,
                sites=plan_site,
                output_sites=["local"],
                output_dir=self.local_storage_dir,
                cleanup="leaf",
                force=True,
                submit=submit,
                cluster=cluster_type
                )
            )
            if wait:
                self.wf.wait()

        except Exception as e:
            print(e)
            sys.exit(-1)


if __name__ == "__main__":
    parser = ArgumentParser(description="Pegasus IO Synthetic Workflow")

    parser.add_argument(
        "-s",
        "--skip-sites-catalog",
        action="store_true",
        help="Skip site catalog creation",
    )
    parser.add_argument(
        "-e",
        "--execution-site",
        metavar="STR",
        type=str,
        default="condorpool",
        help="Execution site name (default: condorpool)",
    )
    parser.add_argument(
        "-c",
        "--workflow-class",
        metavar="STR",
        type=str,
        default="new_chain",
        # required=True,
        help="Workflow structure, chain, fork or custom (default: new_chain)",
    )
    parser.add_argument(
        "-w",
        "--workflow-yml",
        metavar="STR",
        type=str,
        default=None,
        help="YAML file of an existing Pegasus workflow",
    )
    parser.add_argument(
        "-n",
        "--number-jobs",
        metavar="INT",
        type=int,
        default=1,
        help="Number of jobs generated, only valid if chain or fork have been chosen (default: 1)",
    )
    parser.add_argument(
        "-p",
        "--file-path",
        metavar="STR",
        type=str,
        default=None,
        help="Absolute path of input file for the first job",
    )
    parser.add_argument(
        "-b",
        "--bin-path",
        metavar="STR",
        type=str,
        default=None,
        help="Absolute path of the binary you want to use. If this option is not specified, pegasus-keg furnished by Pegasus will be used.",
    )
    parser.add_argument(
        "-f",
        "--workflow-name",
        metavar="STR",
        type=str,
        default="io-synthetic",
        help="Name of the workflow.",
    )
    # parser.add_argument(
    #     "-o",
    #     "--output",
    #     metavar="STR",
    #     type=str,
    #     default="workflow.yml",
    #     help="Output file (default: workflow.yml)",
    # )

    args = parser.parse_args()

    if not args.workflow_class in ["chain", "new_chain", "fork", "custom", "decaf"]:
        parser.error('-c/--workflow-class can only be set to "chain", "new_chain", "fork", "decaf", or "custom".')
    
    if args.workflow_class == "custom" and args.workflow_yml is None:
        parser.error(
            '-c/--workflow-class == "custom" requires -w/--workflow-yml to be set.')

    if args.workflow_class in ["chain", "new_chain", "fork", "decaf"] and (args.number_jobs is None or args.number_jobs < 1):
        parser.error(
            '-c/--workflow-class == "chain" or "fork" requires -n/--number-jobs to be set (and it must be >=1).')

    # if not args.bin_path:
    #     parser.error(
    #         '-b/--bin-path must not be empty.')

    if args.workflow_class == "custom":
        workflow_class = (args.workflow_class, args.workflow_yml)
    elif args.workflow_class in ["chain", "new_chain", "fork", "decaf"]:
        workflow_class = (args.workflow_class, args.number_jobs)
    else:
        parser.error('Unknown parsing argument error')

    workflow = IOSyntheticWorkflow(
        wf_name=args.workflow_name,
        exec_site_name=args.execution_site,
        binary_path=args.bin_path,
        shape=workflow_class,
        waiting_time=[2,2,2,2,2],
        files_size=[1.0,1.0,1.0,1.0,1.0]
    )

    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog()

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()

    print("Creating transformation catalog...")
    workflow.create_transformation_catalog()

    print("Creating replica catalog...")
    workflow.create_replica_catalog(args.file_path)

    print("Creating pipeline workflow dag...")
    workflow.create_workflow()

    workflow.write()
    workflow.run(submit=False, wait=False)