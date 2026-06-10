'''
Author: Peter Willis
Desc: Collection of scripts to aid in the testing of FABRIC experiments. 

THE SLICE MUST ALREADY BE CREATED FOR THIS TO WORK
'''

from fabrictestbed_extensions.fablib.fablib import FablibManager as fablib_manager
from ipaddress import ip_address, IPv4Address, IPv4Network
import datetime
import ntpath

class FabOrchestrator:
    # Constructor, get access to the slice and nodes
    def __init__(self, sliceName):
        '''
        Gain access to the FABRIC slice and its nodes.

        :param sliceName: The name of the slice you are working on.
        '''
        
        try:            
            # Slice
            self.slice = fablib_manager().get_slice(sliceName)

            # Nodes
            self.nodes = self.slice.get_nodes()

            print(f"Slice name: {sliceName}\nSlice and nodes were acquired successfully.")
 
        except Exception as e:
            print(f"Exception: {e}")

    
    def selectedNodes(self, prefixList=None, excludedList=None):
        '''
        Perform an action (execute a command, up/download a file, etc.) on a subset of nodes.
        This is an iterator meant to be used by other functions or in a loop.

        :param prefixList: A naming prefix (ex: C for client) that groups nodes together to run the same configuration.
        :param excludedList: A naming prefix that groups nodes together to NOT run the desired configuration.
        '''
        
        if(prefixList):
            prefixList = tuple(map(str.strip, prefixList.split(",")))

        if(excludedList):
            excludedList = tuple(map(str.strip, excludedList.split(",")))
        
        for node in self.nodes:
            nodeName = node.get_name()

            if((excludedList and nodeName.startswith(excludedList)) or (prefixList and not nodeName.startswith(prefixList))):
                continue
            
            yield node

    
    def executeCommandsParallel(self, command, prefixList=None, excludedList=None, addNodeName=False, returnOutput=False):
        '''
        Execute a command, in parallel using threads, on all or a subset of remote FABRIC nodes.

        :param prefixList: A naming prefix (ex: C for client) that groups nodes together to run the same configuration.
        :param excludedList: A naming prefix that groups nodes together to NOT run the desired configuration.
        :param addNodeName: Add the name of the node to the command. The command MUST include the string format {name} for this to work.
        :param returnOutput: If the stdout/console output should be captured, set to True.
        :returns: The stdout of the command run on each node if set in returnOutput, otherwise None.
        '''

        # Dict to store stdout, if desired.
        cmdOutput = {}

        try:
            #Create execute threads
            execute_threads = {}
            for node in self.selectedNodes(prefixList, excludedList):
                nodeName = node.get_name()
                if(addNodeName is True):
                    finalCommand = command.format(name=nodeName)
                else:
                    finalCommand = command
                
                print(f"Starting command on node {nodeName}")
                print(f'Command to execute: {finalCommand}')
                
                execute_threads[node] = node.execute_thread(finalCommand)

            #Wait for results from threads
            for node,thread in execute_threads.items():
                nodeName = node.get_name()
                print(f"\n==== {nodeName} RESULTS ====")

                stdout,stderr = thread.result()
                print(f"stdout:\n{stdout}")
                print(f"stderr:\n{stderr}")

                if(returnOutput):
                    cmdOutput[nodeName] = stdout

        except Exception as e:
            print(f"Exception: {e}")

        if(returnOutput):
            return cmdOutput

        return

    
    def uploadDirectoryParallel(self, directory, remoteLocation=None, prefixList=None, excludedList=None):
        '''
        Upload a directory, in parallel using threads, onto all or a subset of remote FABRIC nodes.

        :param directory: The path to the directory you wish to upload.
        :param remoteLocation: The full path of the remote directory you wish to place the uploaded directory.
        :param prefixList: A naming prefix (ex: C for client) that groups nodes together to run the same configuration.
        :param excludedList: A naming prefix that groups nodes together to NOT run the desired configuration.
        '''
        
        if(remoteLocation is None):
            remoteLocation = "/home/rocky"
        
        print(f'Directory to upload: {directory}\nPlaced in: {remoteLocation}')

        try:
            #Create execute threads
            execute_threads = {}
            for node in self.selectedNodes(prefixList, excludedList):
                print(f"Starting upload on node {node.get_name()}")
                execute_threads[node] = node.upload_directory_thread(directory, remoteLocation)

            #Wait for results from threads
            for node,thread in execute_threads.items():
                print(f"Waiting for result from node {node.get_name()}")
                output = thread.result()
                print(f"Output: {output}")

        except Exception as e:
            print(f"Exception: {e}")

        return    

    
    def uploadFileParallel(self, file, remoteLocation=None, prefixList=None, excludedList=None):
        '''
        Upload a file, in parallel using threads, onto all or a subset of remote FABRIC nodes.

        :param file: The path to the file you wish to upload.
        :param remoteLocation: The full path of the remote directory you wish to place the uploaded directory.
        :param prefixList: A naming prefix (ex: C for client) that groups nodes together to run the same configuration.
        :param excludedList: A naming prefix that groups nodes together to NOT run the desired configuration.
        '''
        
        if(remoteLocation is None):
            fileName = ntpath.basename(file)
            remoteLocation = f"/home/rocky/{fileName}"
        
        print(f'File to upload: {file}\nPlaced in: {remoteLocation}')

        try:
            #Create execute threads
            execute_threads = {}
            for node in self.selectedNodes(prefixList, excludedList):
                print(f"Starting upload on node {node.get_name()}")
                execute_threads[node] = node.upload_file_thread(file, remoteLocation)

            #Wait for results from threads
            for node,thread in execute_threads.items():
                print(f"Waiting for result from node {node.get_name()}")
                output = thread.result()
                print(f"Output: {output}")

        except Exception as e:
            print(f"Exception: {e}")

        return

    
    def downloadFilesParallel(self, localLocation, remoteLocation, prefixList=None, excludedList=None, addNodeName=False):
        '''
        Download a file, in parallel using threads, from all or a subset of remote FABRIC nodes.

        :param localLocation: The path to where the downloaded file should be stored, including the filename.
        :param remoteLocation: The full path of the file to be downloaded.
        :param prefixList: A naming prefix (ex: C for client) that groups nodes together to run the same configuration.
        :param excludedList: A naming prefix that groups nodes together to NOT run the desired configuration.
        :param addNodeName: Add the name of the node to the file path (local and/or remote). The path MUST include the string format {name} for this to work.
        '''
        
        #Create execute threads
        execute_threads = {}
        for node in self.selectedNodes(prefixList, excludedList):
            nodeName = node.get_name()
            if(addNodeName is True):
                finalRemoteLocation = remoteLocation.format(name=nodeName)
                finalLocalLocation =  localLocation.format(name=nodeName)
            else:
                finalRemoteLocation = remoteLocation
                finalLocalLocation =  localLocation

            print(f"Starting download on node {nodeName}")
            print(f'File to download: {finalRemoteLocation}')
            print(f'Location of download: {finalLocalLocation}')

            execute_threads[node] = node.download_file_thread(finalLocalLocation, finalRemoteLocation)

        #Wait for results from threads
        for node,thread in execute_threads.items():
            print(f"Waiting for result from node {node.get_name()}")
            
            try:
                output = thread.result()
            except Exception as e:
                print(f"Exception: {e}")
            
            print(f"Output: {output}")

        return

    
    def addIPAddressToInterface(self, node, interface, ipAddress, mask):
        '''
        Add a NetworkManager-controlled IPv4 address to a node on a specific interface.
        This should only be used if NetworkManager is required, there are easier ways
        of adding addressing in FABRIC that does not involve NetworkManager.

        :param node: The FABRIC Node object to modify.
        :param interface: The name of the interface to add the IPv4 address.
        :param ipAddress: The dotted-decimal IPv4 address without any mask information.
        :param mask: The subnet mask of the IPv4 address using the CIDR prefix (no slash).
        '''

        command = ("sudo nmcli con add type ethernet " 
                  f"con-name {interface} ifname {interface} "
                  f"autoconnect yes ipv4.method manual ipv4.addresses {ipAddress}/{mask} "
                  f"&& sudo nmcli dev set {interface} managed yes")
        
        node.execute(command)
        print(f"Interface {interface} has been configured with IP address {ipAddress}/{mask}")
        
        return

    
    def saveSSHCommands(self):
        '''
        Grab the SSH command for each node in the topology and save it to a file in the local directory.
        '''
        
        with open(f"{self.slice.get_name()}_ssh_cmds.txt", "w") as sshFile:
            for node in self.nodes:
                sshFile.write(f"{node.get_name()}:\n")
                sshFile.write(f"{node.get_ssh_command()}\n")

        return

    
    def renewSlice(self, daysToAdd):
        '''
        Renew the slice for a set number of days.

        :param daysToAdd: The number of days to add to the slice lifetime.
        '''
        
        endDate = (datetime.datetime.now() + datetime.timedelta(days=daysToAdd)).strftime("%Y-%m-%d %H:%M:%S %z") + "+0000"
        self.slice.renew(endDate)
        
        return

    
    def getInterfaceSubnet(self, intfName):
        '''
        Return the subnet of a FABRIC node's interface
        '''
        
        intf = self.slice.get_interface(intfName)
        
        subnet = IPv4Network(f"{intf.get_ip_addr()}/24", strict=False)
        
        return subnet
