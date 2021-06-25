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
from typing import Optional, Tuple, Union

logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API ------------------------------------------------------


class IOSyntheticWorkflow(object):
    wf = None
    sc = None
    tc = None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None

    # --- Init ----------------------------------------------------------------
    def __init__(self,
                 shape: Optional[Tuple[str, Union[int, str]]] = ("chain", 1),
                 exec_site_name: Optional[str] = "condorpool"
                 ) -> None:
        self.wf_name = "io-synthetic"
        self.wid = self.wf_name + "-" + datetime.now().strftime("%s")
        self.dagfile = self.wid+".yml"

        self.wf_dir = str(Path(__file__).parent.resolve())
        self.exec_site_name = exec_site_name

        self.shape = shape            

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
        self.wf.write()

    # --- Configuration (Pegasus Properties) ----------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()

        # props["pegasus.monitord.encoding"] = "json"
        # self.properties["pegasus.integrity.checking"] = "none"
        return

    # --- Site Catalog --------------------------------------------------------
    def create_sites_catalog(self):
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
    def create_transformation_catalog(self):
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
            keg = (
                Transformation("keg", site="cori", pfn=os.path.join(github_location, "bin/wrapper.sh"), is_stageable=True)
                .add_pegasus_profile(
                    cores="1",
                    runtime="1800",
                    glite_arguments="--qos=debug --constraint=haswell --licenses=SCRATCH",
                )
            )

            self.tc.add_transformations(pegasus_transfer, pegasus_dirmanager, pegasus_cleanup, system_chmod)

        self.tc.add_transformations(keg)


    # --- Create Workflow -----------------------------------------------------

    def create_workflow(self):
        if self.shape[0] == "chain":
            self.create_workflow_chain()
        elif self.shape[0] == "fork":
            self.create_workflow_fork()
        else:
            self.create_workflow_custom()

    def create_workflow_chain(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        ## First job
        f1 = File("j1.txt")
        job1 = (
            Job("keg")
            .add_args("-o", f1, "-T", 5, "-G", 100, "-u", "M")
            .add_outputs(f1, stage_out=False, register_replica=False)
        )
        self.wf.add_jobs(job1)

        # Security check to ensure nb of jobs is positive >= 1
        nb_jobs = max(self.shape[1], 1)

        for i in range(1, nb_jobs):
            fi = File("f{}.txt".format(i))
            keg = (
                Job("keg")
                .add_args("-i", f1, "-o", fi, "-T", 5, "-G", 100, "-u", "M")
                .add_inputs(f1)
                .add_outputs(fi, stage_out=False, register_replica=False)
            )
            f1 = fi
            self.wf.add_jobs(keg)

    def create_workflow_fork(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        ## First job
        f1 = File("j1.txt")
        job1 = (
            Job("keg")
            .add_args("-o", f1, "-T", 5, "-G", 100, "-u", "M")
            .add_outputs(f1, stage_out=False, register_replica=False)
        )
        self.wf.add_jobs(job1)

        # Security check to ensure nb of jobs is positive >= 1
        nb_jobs = max(self.shape[1], 1)

        flast = File("f{}.txt".format(nb_jobs+1))
        joblast = (
            Job("keg")
            .add_args("-o", flast, "-T", 5, "-G", 100, "-u", "M")
            .add_outputs(flast, stage_out=False, register_replica=False)
        )

        for i in range(1, nb_jobs+1):
            fi = File("f{}.txt".format(i))
            keg = (
                Job("keg")
                .add_args("-i", f1, "-o", fi, "-T", 5, "-G", 100, "-u", "M")
                .add_inputs(f1)
                .add_outputs(fi, stage_out=False, register_replica=False)
            )

            joblast.add_args("-i", fi)
            joblast.add_inputs(fi)

            self.wf.add_jobs(keg)

        ## We add the last job
        self.wf.add_jobs(joblast)

    # --- Create Workflow -----------------------------------------------------
    def create_workflow_custom(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        file = self.shape[1]

        with open(file, 'r') as f:
            try:
                data = yaml.safe_load(f)

                for job in data['jobs']:
                    keg = Job("keg")
                    keg.add_args("-G", 100, "-u", "M")

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
            self.wf.plan(
                dir=self.wf_dir,
                relative_dir=self.wid,
                sites=plan_site,
                output_sites=["local"],
                output_dir=self.local_storage_dir,
                cleanup="leaf",
                force=True,
                submit=submit
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
        default="chain",
        required=True,
        help="Workflow structure, chain, fork or custom (default: chain)",
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

    # parser.add_argument(
    #     "-o",
    #     "--output",
    #     metavar="STR",
    #     type=str,
    #     default="workflow.yml",
    #     help="Output file (default: workflow.yml)",
    # )

    args = parser.parse_args()

    if not args.workflow_class in ["chain", "fork", "custom"]:
        parser.error('-c/--workflow-class can only be set to "chain", "fork" or "custom".')
    
    if args.workflow_class == "custom" and args.workflow_yml is None:
        parser.error(
            '-c/--workflow-class == "custom" requires -w/--workflow-yml to be set.')

    if args.workflow_class in ["chain", "fork"] and (args.number_jobs is None or args.number_jobs < 1):
        parser.error(
            '-c/--workflow-class == "chain" or "fork" requires -n/--number-jobs to be set (and it must be >=1).')

    if args.workflow_class == "custom":
        workflow_class = (args.workflow_class, args.workflow_yml)
    elif args.workflow_class in ["chain", "fork"]:
        workflow_class = (args.workflow_class, args.number_jobs)
    else:
        parser.error('Unknown parsing argument error')

    workflow = IOSyntheticWorkflow(
        exec_site_name=args.execution_site, shape=workflow_class)

    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog()

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()

    print("Creating transformation catalog...")
    workflow.create_transformation_catalog()

    print("Creating pipeline workflow dag...")
    workflow.create_workflow()

    workflow.write()
    workflow.run(submit=False, wait=False)
