#!/usr/bin/env python3
import os
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from argparse import ArgumentParser

logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API ------------------------------------------------------
from Pegasus.api import *

class IOSyntheticWorkflow(object):
    wf = None
    sc = None
    tc = None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None

    # --- Init ----------------------------------------------------------------
    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_name = "io-synthetic"
        self.wid = "io-synth-" + datetime.now().strftime("%s")
        self.wf_dir = str(Path(__file__).parent.resolve())

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
    def create_sites_catalog(self, exec_site_name="condorpool"):
        self.sc = SiteCatalog()

        self.shared_scratch_dir = os.path.join(self.wf_dir, "{}/scratch".format(self.wid))
        self.local_storage_dir = os.path.join(self.wf_dir, "{}/output".format(self.wid))

        local = Site("local").add_directories(
            Directory(Directory.SHARED_SCRATCH, self.shared_scratch_dir).add_file_servers(
                FileServer("file://" + self.shared_scratch_dir, Operation.ALL)
            ),
            Directory(Directory.LOCAL_STORAGE, self.local_storage_dir).add_file_servers(
                FileServer("file://" + self.local_storage_dir, Operation.ALL)
            ),
        )

        exec_site = (
            Site(exec_site_name)
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        )

        self.sc.add_sites(local, exec_site)

    # --- Transformation Catalog (Executables and Containers) -----------------
    def create_transformation_catalog(self, exec_site_name="condorpool"):
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

        self.tc.add_transformations(keg)

    # --- Create Workflow -----------------------------------------------------
    def create_workflow(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        # -o namefile1 namefile2 -G sizefile1 sizefile2 -u M
        k1_out = File("k1.txt")

        keg_job1 = (
            Job("keg")
            .add_args("-o", k1_out, "-T", 5, "-G", 100, "-u", "M")
            .add_outputs(k1_out, stage_out=True, register_replica=False)
        )

        k2_out = File("k2.txt")

        keg_job2 = (
            Job("keg")
            .add_args("-i", k1_out, "-o", k2_out, "-T", 5, "-G", 100, "-u", "M")
            .add_inputs(k1_out)
            .add_outputs(k2_out, stage_out=True, register_replica=False)
        )


        self.wf.add_jobs(keg_job1, keg_job2)

    # --- Run Workflow -----------------------------------------------------
    def run(self, submit=False):
        try:
            self.wf.plan(
                dir=self.wf_dir,
                relative_dir=self.wid,
                output_sites=["local"],
                output_dir=self.local_storage_dir,
                cleanup="leaf",
                submit=submit
            )  # .wait()
        except Exception as e:
            print(e)


if __name__ == "__main__":
    parser = ArgumentParser(description="Pegasus IO Synthetic Workflow")

    parser.add_argument(
        "-s",
        "--skip_sites_catalog",
        action="store_true",
        help="Skip site catalog creation",
    )
    parser.add_argument(
        "-e",
        "--execution_site_name",
        metavar="STR",
        type=str,
        default="condorpool",
        help="Execution site name (default: condorpool)",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="STR",
        type=str,
        default="workflow.yml",
        help="Output file (default: workflow.yml)",
    )

    args = parser.parse_args()

    workflow = IOSyntheticWorkflow(args.output)

    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog(args.execution_site_name)

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()
    
    print("Creating transformation catalog...")
    workflow.create_transformation_catalog(args.execution_site_name)

    print("Creating pipeline workflow dag...")
    workflow.create_workflow()

    workflow.write()
    workflow.run(submit=False)
