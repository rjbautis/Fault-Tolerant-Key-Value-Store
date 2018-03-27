import os
import requests
import hashlib
import json
import time
import threading
from flask import Flask, request, jsonify
app = Flask(__name__)


# Return the checkpoints for the initial partitions' location on the unit circle
def make_check_points():
    global range_dict       # Stores key=hash_location and value=partition_id
    global find_hash_dict   # Stores key=partition_id and value=hash_location
    l = limit
    range_size = l / global_num_partitions
    range_array = []
    range_dict.clear()
    find_hash_dict.clear()

    # Fill up the list that will hold the 'checkpoints' i.e. the locations of each partition on the unit circle
    for i in range(0, global_num_partitions):
        range_array.insert(0, int(l - (range_size) * i))
    # Fill up 1st dictionary such that key=hash_location and value=partition_id
    # Fill up 2nd dictionary such that key=partition_id and value=hash_location
    for i in range(0, len(range_array)):
        range_dict[range_array[i]] = i
        find_hash_dict[i] = range_array[i]
    return range_array

# Assigns to each node a partition_id, creates a global list of all partition_ids available, updates dict containing members per id
def partition_id_assigner():
    global partition_id_list
    global partition_members_per_id
    temp_dict = {}
    temp_arr = []
    counter = 0
    assigned_id = 0
    # Assign partition_id to each node
    for node in nodes:
        temp_dict[node] = assigned_id
        temp_arr.append(node)
        counter += 1
        if counter == num_replicas:
            partition_members_per_id[assigned_id] = temp_arr         # Update partition_members_per_id with list
            partition_id_list.append(assigned_id)                    # Update global list of all partition_ids 
            temp_arr=[]
            assigned_id += 1
            counter = 0
    return temp_dict

def consistent_hash(key):
    key = key.encode('utf8')
    k = hashlib.md5(key).hexdigest()
    num = int(k, 16)
    hash_num = num % limit
    return hash_num

def intialize_background():
    try:
        server_URL = 'http://' + os.environ['ip_port'] + '/kvs/get_number_of_keys'
        r = requests.get(server_URL, timeout=0.4)
    except Exception as e:
        pass
    time.sleep(1)

# Background service to ping all other nodes in same partition
@app.before_first_request
def ping_nodes():
    def ping():
        while True:
            for server in partition_members_per_id[partition_id]:
                if server == os.environ['ip_port']: continue
                try:
                    server_URL = "http://" + server + "/kvs/return_this_storage"
                    r = requests.get(server_URL, timeout=0.4)
                except Exception as e:
                    continue
                # r returns the storage of desired server   
                for key,value in storage.items():
                    # Case where the value in server storage at key is not updated
                    try:
                        if value is not r[key]:
                            server_URL = "http://" + server + "/kvs/replicate_node"
                            r = requests.put(server_URL, data={'key': key, 'value': value[0], 'causal_payload': value[1], 'timestamp': value[2]}, timeout=0.4)
                    # Case where key does not exist within servers' storage
                    except Exception as e:  
                        # Call replicate this guy  
                        server_URL = "http://" + server + "/kvs/replicate_node"
                        r = requests.put(server_URL, data={'key': key, 'value': value[0], 'causal_payload': value[1], 'timestamp': value[2]}, timeout=0.4)
            time.sleep(0.5)
    thread = threading.Thread(target=ping)
    thread.daemon = True
    thread.start()

@app.route('/kvs', methods=['GET', 'PUT'])
def kvs():
    global arr
    global partition_id
    #1
    if request.method == 'GET':
        key = request.args['key']
        cp = request.args.get('causal_payload')
        hash_location = consistent_hash(key)

        # Find successor node that we will place key into
        i = 0
        while(hash_location > arr[i]): i += 1
        successor_partition = arr[i]

        # If the successor node's ip address == local ip address, then no need to forward
        if range_dict[successor_partition] == partition_id:
            if key not in storage:
                return jsonify(msg="error", error="key does not exist"), 404
            else:
                return jsonify(msg="success", value=storage[key][0], partition_id=partition_id, causal_payload=storage[key][1], timestamp=storage[key][2]), 200
        else:
            for server in partition_members_per_id[range_dict[successor_partition]]:
                # Try get request on all servers. as soon as one successfully replies, exit loop and return answer
                try:
                    server_URL = "http://" + server + "/kvs?key="+ key
                    r = requests.get(server_URL, timeout=0.8)
                    return r.text, r.status_code
                except Exception as e:
                    continue

    #2
    if request.method == 'PUT':
        try:
            key = request.form['key']
            val = request.form['value']
            cp = request.form['causal_payload']
        except Exception as e:
            return jsonify(msg='Error', machine=os.environ['ip_port']), 400

        # If causal_payload is empty string, then key is new to the network. Has all-zero payload
        if cp == "":
            for i in range(int(os.environ["K"])): cp += "0."
            cp = cp[:-1]
        hash_location = consistent_hash(key)
        # Find successor node that we will place key into
        i = 0 
        while(hash_location > arr[i]): i += 1
        successor_partition = arr[i]

        # Holds status of key being in storage or not
        replaced = -1

        # If the successor partition_id == local partition_id (i.e. local node), then no need to forward to another partition
        if range_dict[successor_partition] == partition_id:
            # Part 1: Update Casual Payload
            # step A: Convert CP to an array
            current_vc = []
            for x in cp.split('.'):  
                interger = int(x)
                current_vc.append(interger)
            # Step B: Determine location in the computer 
            # Find which partiton our ip is located in
            my_Partition_key = 0
            for k, v in partition_id_finder.items():   
                if k == os.environ['ip_port']:
                    my_Partition_key = v
                    break
            # Then use that index to get the partition list
            my_partition_list = partition_members_per_id[my_Partition_key]
            # To find index of this local node
            Comp_number = 0
            for item in my_partition_list:
                if item != os.environ['ip_port']:
                    Comp_number += 1
                else:
                    break
            # Then take that index and update it respectively by 1
            current_vc[Comp_number] += 1
            # Convert back into 0.0.0 format
            cp = ""
            for i in range(int(os.environ["K"])):
                item = str(current_vc[i])
                cp += item
                cp +='.'
            cp = cp[:-1]
         
            # Check if key exists in storage already
            replaced = 0 if key not in storage else 1 
            ts = time.time()
            storage[key] = [val, cp, ts]
            # For every node in the partition except this one, replicate it with most recent data
            for server in partition_members_per_id[partition_id]: 
                if server == os.environ['ip_port']: continue
                try:
                    server_URL = "http://" + server + "/kvs/replicate_node"
                    r = requests.put(server_URL, data={'key': key, 'value': val, 'causal_payload': cp, 'timestamp': ts}, timeout=0.4)
                except Exception as e:
                    continue

            return jsonify(replaced=replaced, msg="success", partition_id=partition_id, causal_payload=storage[key][1], timestamp=storage[key][2]), 200
        # Otherwise, forward the key to the appropriate partition where the key should go into
        else:
            # Assign first node in partition member's list to be the node we forward to
            for server in partition_members_per_id[range_dict[successor_partition]]:
                try:
                    server_URL = "http://" + server + "/kvs"
                    r = requests.put(server_URL, data={'key': key, 'value': val, 'causal_payload': cp},timeout=0.5)
                    return r.text, r.status_code
                except Exception as e:
                    continue


@app.route('/kvs/replicate_node', methods=['PUT'])
def replicate_node():
    global storage
    try:
        most_recent_key = request.form['key']
        most_recent_val = request.form['value']
        most_recent_cp = request.form['causal_payload']
        most_recent_ts = float(request.form['timestamp'])
    except Exception as e:
        return jsonify(error="Replication error: missing appropriate parameters"), 400
    # Step 2
    # Convert Payloads to arrays
    # Recent(the one that forwarded)
    most_recent_vc = []
    for x in most_recent_cp.split('.'): 
        interger = int(x)
        most_recent_vc.append(interger)
    # Current(the one currently in the system)
    my_vc = []
    my_cp = ""
    if most_recent_key not in storage:
        for i in range(int(os.environ["K"])): 
            my_cp += "0."
        my_cp = my_cp[:-1]
        i = 0 
    else:
        my_cp = storage[most_recent_key][1]
    for f in my_cp.split('.'):  
        my_interger = int(f)
        my_vc.append(my_interger)

    # Check if the two vector clocks are concurrent
    not_concurrent = True
    for i in range(int(os.environ["K"])):
        if not_concurrent is False:
            break
        if most_recent_vc[i] < my_vc[i]:
            not_concurrent = False
    # If you can compare the two payloads and it is less than then update to value
    if not_concurrent is True:
        storage[most_recent_key] = [most_recent_val, most_recent_cp, most_recent_ts]
    # Else, compare timestamp
    # If current timestamp is less than forwarded timestamp, then update 
    # Else do nothing
    elif not_concurrent is False:
        if most_recent_key not in storage:
            storage[most_recent_key] = [most_recent_val, most_recent_cp, most_recent_ts]
        elif most_recent_ts > storage[most_recent_key][2]:
            storage[most_recent_key] = [most_recent_val, most_recent_cp, most_recent_ts]
    return jsonify(msg="success"), 200


@app.route('/kvs/view_update', methods=['PUT'])
def view_update():
    global nodes
    global arr
    global range_dict
    global find_hash_dict
    global partition_id_list
    global partition_members_per_id
    global partition_id_finder
    global partition_id
    test = []
    flag = False
    # Temporary arrays for storing keys/values in case of redistribution
    rkey = []
    rvalue = []
    dead_node_flag = False

    if request.method == 'PUT':
        try:
            ip_port = str(request.form['ip_port'])
            change_type = request.form['type']
        except Exception as e:
            return jsonify(msg='view update error: need to specify value ip:port or type'), 400
        
        # See if computer is alive in the first place. i.e. see if we can call get_number_of_keys
        # If dead, then remove it from local node's view and update all other views
        try:
            test_URL = "http://" + ip_port + "/kvs/get_number_of_keys"
            is_alive  = requests.get(test_URL, timeout=0.8)
        except Exception as e:
            dead_node_flag = True
            pass

        if change_type == 'add':
            unfilled_partition = -1
            os.environ['VIEW'] = os.environ['VIEW'] + "," + ip_port
            nodes = os.environ['VIEW'].split(",")
            # Flag to see if current partitions are filled
            need_new_partition = True

            for key,value in partition_members_per_id.items():
                if len(value) == int(os.environ['K']): continue
                need_new_partition = False
                unfilled_partition = key

            # If a new partition is needed
            if need_new_partition:
                new_partition_id = partition_id_list[-1] + 1
                p = 'p_' + str(new_partition_id)
                hash_location = consistent_hash(p)           # Hash the ip_port, not the new_partition_id

                # Add the new ip_port to all global arrays and dictionaries
                arr.append(hash_location)
                arr.sort()

                range_dict[hash_location] = new_partition_id
                find_hash_dict[new_partition_id] = hash_location

                partition_id_list.append(new_partition_id)
                partition_id_list.sort()

                partition_members_per_id[new_partition_id] = [ip_port]
                partition_id_finder[ip_port] = new_partition_id

                # Update the view and global variables for every other node except this one
                for server in nodes:
                    if server == os.environ['ip_port']: continue
                    server_URL = "http://" + server + "/kvs/update_view"
                    r = requests.put(server_URL, data={
                        'view': os.environ['VIEW'], 'arr': json.dumps(arr, sort_keys=True), 'range_dict': json.dumps(range_dict, sort_keys=True), \
                        'find_hash_dict': json.dumps(find_hash_dict, sort_keys=True), 'partition_id_list': json.dumps(partition_id_list, sort_keys=True), \
                        'partition_members_per_id': json.dumps(partition_members_per_id, sort_keys=True), 
                        'partition_id_finder': json.dumps(partition_id_finder, sort_keys=True), 'K': os.environ['K']
                        })

                i = 0
                while(hash_location >= arr[i]): i += 1
                successor_partition = arr[i]

                # Redistribute the keys among the nodes
                # If successor partition is partition of local node, then remove valid keys from all nodes in that partition
                if range_dict[successor_partition] == partition_id:
                    # First remove keys from this local node
                    for key, value in storage.items():
                        key_hash_location = consistent_hash(key)
                        if key_hash_location < hash_location:
                            rkey.append(key)
                            rvalue.append(value)

                    for key in rkey: 
                        storage.pop(key)

                    # Then remove keys for the other nodes in old partition
                    for server in partition_members_per_id[partition_id]:
                        if server == os.environ['ip_port']: continue
                        server_URL = "http://" + server + "/kvs/remove_keys"
                        r = requests.put(server_URL, data={'type': 'list', 'keys': json.dumps(rkey)})

                    # Insert removed keys into nodes that are in the same partition as the new node (using its hash_location)
                    for server in partition_members_per_id[range_dict[hash_location]]:
                        server_URL = "http://" + server + "/kvs/replicate_node"
                        for i in range(0, len(rkey)):
                            r = requests.put(server_URL, data={'key': rkey[i], 'value': rvalue[i][0], \
                                                                'causal_payload': rvalue[i][1], 'timestamp': rvalue[i][2]}) 
                else:
                    # Go to the first node in list of members for that successor partition
                    server_URL = "http://" + partition_members_per_id[range_dict[successor_partition]][0] + "/kvs/redistribute"
                    r = requests.put(server_URL, data={'change_type': change_type, 'hash_location': hash_location}) 
            
            # Else if there exists a partition that is not full
            else:
                # Add new ip_port to members list of that partition
                partition_members_per_id[unfilled_partition].append(ip_port)
                partition_id_finder[ip_port] = unfilled_partition     
                # Update the view and global variables for every other node except this one
                for server in nodes:
                    if server == os.environ['ip_port']: continue
                    server_URL = "http://" + server + "/kvs/update_view"
                    r = requests.put(server_URL, data={
                        'view': os.environ['VIEW'], 'arr': json.dumps(arr, sort_keys=True), 'range_dict': json.dumps(range_dict, sort_keys=True), \
                        'find_hash_dict': json.dumps(find_hash_dict, sort_keys=True), 'partition_id_list': json.dumps(partition_id_list, sort_keys=True), \
                        'partition_members_per_id': json.dumps(partition_members_per_id, sort_keys=True), 
                        'partition_id_finder': json.dumps(partition_id_finder, sort_keys=True), 'K': os.environ['K']
                        })
                # If unfilled_partition's id equals this node's partition_id, then replicate added node with local storage
                if unfilled_partition == partition_id: 
                    for key, value in storage.items():
                        rkey.append(key)
                        rvalue.append(value)
                    server_URL = "http://" + server + "/kvs/replicate_node"
                    for i in range(0, len(rkey)):
                        r = requests.put(server_URL, data={'key': rkey[i], 'value': rvalue[i][0], \
                                                            'causal_payload': rvalue[i][1], 'timestamp': rvalue[i][2]}) 
                # Else, replicate data from first node in unfilled_partition by forwarding ip_port to that node 
                else:
                    server_URL = "http://" + partition_members_per_id[unfilled_partition][0] + "/kvs/forward_request"
                    r = requests.put(server_URL, data={'ip_port': ip_port})
            return jsonify(msg='success', partition_id=partition_id_finder[ip_port], number_of_partitions=len(partition_id_list)), 200

        if change_type == 'remove':
            substring = ""
            # Store old VIEW env for iterating
            old_view_env = os.environ['VIEW'].split(",")
            # Reconstruct the VIEW so that it no longer contains the one we want to remove
            reconstruct = os.environ['VIEW'].split(",")
            for server in reconstruct:
                if server == ip_port: continue
                substring = substring + server + ","

            os.environ['VIEW'] = substring[:-1]
            nodes = os.environ['VIEW'].split(",")

            # Remove node from partition dictionaries
            id_of_input_node = partition_id_finder.pop(ip_port)
            partition_members_per_id[id_of_input_node].remove(ip_port)

            # If the partition is empty after removing the node, update everything AND redistribute the keys of that node
            if not partition_members_per_id[id_of_input_node]:
                hash_to_remove = find_hash_dict.pop(id_of_input_node)
                # Successor node where keys will be redistributed if the partition is empty
                i = 0
                while(hash_to_remove >= arr[i]): i += 1
                successor_partition = range_dict[arr[i]]

                arr.remove(hash_to_remove)
                partition_id_list.remove(id_of_input_node)
                partition_members_per_id.pop(id_of_input_node)
                range_dict.pop(hash_to_remove)

                # Iterate through all nodes EXCEPT the local node (which we already did) and update view
                for server in old_view_env:
                    if server == os.environ['ip_port']:
                        continue
                    elif (server == ip_port) and dead_node_flag is True:
                        continue
                    else:
                        server_URL = "http://" + server + "/kvs/update_view"
                        r = requests.put(server_URL, data={ 'change_type': change_type, \
                            'view': os.environ['VIEW'], 'arr': json.dumps(arr,sort_keys=True), 'range_dict': json.dumps(range_dict,sort_keys=True), \
                            'find_hash_dict': json.dumps(find_hash_dict,sort_keys=True), 'partition_id_list': json.dumps(partition_id_list,sort_keys=True), \
                            'partition_members_per_id': json.dumps(partition_members_per_id,sort_keys=True), 
                            'partition_id_finder': json.dumps(partition_id_finder,sort_keys=True), 'K': os.environ['K']
                            })
                if dead_node_flag is False:
                    # If successor_partition != partition_id, then redistribute the keys to successor partition
                    if successor_partition != partition_id:
                        server_URL = "http://" + ip_port + "/kvs/redistribute"
                        r = requests.put(server_URL, data={'change_type': change_type, 'successor_partition': successor_partition})
                    # Else, update this node locally and call replicate_node on all OTHER nodes in this partition 
                    else:
                        server_URL = "http://" + ip_port + "/kvs/return_this_guys_keys_and_values"
                        r = requests.get(server_URL)
                        # Grab keys/values from the node we want to delete
                        response = r.json()
                        for kv in response:
                            kv[1][2] = float(kv[1][2])             # Turn timestamp from string back into float
                            storage[kv[0]] = kv[1]                 # Place keys into local node
                            rkey.append(kv[0])
                            rvalue.append(kv[1])
                        # Replicate other nodes in this (successor) partition
                        for server in partition_members_per_id[successor_partition]:
                            if server == os.environ['ip_port']: continue
                            server_URL = "http://" + server + "/kvs/replicate_node"
                            for i in range(0, len(rkey)):
                                r = requests.put(server_URL, data={'key': rkey[i], 'value': rvalue[i][0], \
                                                                    'causal_payload': rvalue[i][1], 'timestamp': rvalue[i][2]}) 
            # Else, the partition is not empty after removing node. No need to redistribute any keys. Just update view
            else:
                for server in old_view_env:
                    if server == os.environ['ip_port']:
                        continue
                    elif (server == ip_port) and dead_node_flag is True:
                        continue
                    else:
                        server_URL = "http://" + server + "/kvs/update_view"
                        r = requests.put(server_URL, data={ 'change_type': change_type, \
                            'view': os.environ['VIEW'], 'arr': json.dumps(arr,sort_keys=True), 'range_dict': json.dumps(range_dict,sort_keys=True), \
                            'find_hash_dict': json.dumps(find_hash_dict,sort_keys=True), 'partition_id_list': json.dumps(partition_id_list,sort_keys=True), \
                            'partition_members_per_id': json.dumps(partition_members_per_id,sort_keys=True), 
                            'partition_id_finder': json.dumps(partition_id_finder,sort_keys=True), 'K': os.environ['K']
                            })
            # Check if we need to move around nodes between partitions to ensure the minimum number of partitions
            node_to_be_moved = ""
            # Flag to check if redistribution of nodes across partitions is necessary
            r_flag = False
            # Acceptor flag to see if length of partition_member list for local node is 1 or not
            a_flag = False
            # Find one partition that is not the most recently editted but has length < K
            for i in partition_id_list:
                if i == id_of_input_node: continue
                if len(partition_members_per_id[i]) < int(os.environ['K']):
                    r_flag = True
                    node_to_be_moved = partition_members_per_id[i][0]

            # Check if the length of current node's partition_members list is equal to 1
            if len(partition_members_per_id[partition_id]) == 1 and (node_to_be_moved == os.environ['ip_port']):
                a_flag = True
                # Otherwise, the node to be moved is 1st node in members list of inputted
                node_to_be_moved = partition_members_per_id[id_of_input_node][0]

            if r_flag:
                # Technically, id_of_input_node is not equal to partition_id if flag is true. We are just changing value of existing var for convenience
                if a_flag:
                    id_of_input_node = partition_id 

                # Store partition id of node_to_be_moved
                i = partition_id_finder[node_to_be_moved]
                # Remove node_to_be_moved from partition_members_per_id[id]
                partition_members_per_id[i].remove(node_to_be_moved)
                partition_members_per_id[id_of_input_node].append(node_to_be_moved)
                partition_id_finder[node_to_be_moved] = id_of_input_node

                # If old list containing node_to_be_moved is now empty, redistribute the keys
                if not partition_members_per_id[i]:
                    # Update global arrays and dictionaries without i 
                    hash_to_remove = find_hash_dict.pop(i)
                    arr.remove(hash_to_remove)
                    range_dict.pop(hash_to_remove)
                    partition_id_list.remove(i)
                    partition_members_per_id.pop(i)

                    if id_of_input_node != partition_id:
                        # Redistribute grabbed keys to the appropriate partition they belong in using 1st node in that partition
                        server_URL = "http://" + node_to_be_moved + "/kvs/redistribute"
                        r = requests.put(server_URL, data={'change_type': change_type, 'successor_partition': id_of_input_node})        
                    else:
                        # Grab all keys from node_to_be_moved
                        server_URL = "http://" + node_to_be_moved + "/kvs/return_this_guys_keys_and_values"
                        r = requests.get(server_URL)
                        # Grab keys/values from the node we want to delete and redistribute locally
                        response = r.json()            
                        for kv in response:
                            kv[1][2] = float(kv[1][2])             # Turn timestamp from string back into float
                            storage[kv[0]] = kv[1]
                            rkey.append(kv[0])
                            rvalue.append(kv[1])

                        for server in partition_members_per_id[id_of_input_node]:
                            if server == os.environ['ip_port']:
                                continue
                            elif (server == ip_port) and dead_node_flag is True:
                                continue
                            else:
                                server_URL = "http://" + server + "/kvs/replicate_node"
                                for key,value in storage.items():
                                    r = requests.put(server_URL, data={'key': key, 'value': value[0], \
                                                                        'causal_payload': value[1], 'timestamp': value[2]}) 
                    # Update the views of all nodes
                    for server in old_view_env:
                        if server == os.environ['ip_port']:
                            continue
                        elif (server == ip_port) and dead_node_flag is True:
                            continue
                        else:
                            server_URL = "http://" + server + "/kvs/update_view"
                            r = requests.put(server_URL, data={ 'change_type': change_type, \
                                'view': os.environ['VIEW'], 'arr': json.dumps(arr,sort_keys=True), 'range_dict': json.dumps(range_dict,sort_keys=True), \
                                'find_hash_dict': json.dumps(find_hash_dict,sort_keys=True), 'partition_id_list': json.dumps(partition_id_list,sort_keys=True), \
                                'partition_members_per_id': json.dumps(partition_members_per_id,sort_keys=True), 
                                'partition_id_finder': json.dumps(partition_id_finder,sort_keys=True), 'K': os.environ['K']
                                })
                else:
                    # Iterate through all nodes EXCEPT the local node (which we will delete after these are done) and update view
                    for server in old_view_env:
                        if server == os.environ['ip_port']:
                            continue
                        elif (server == ip_port) and dead_node_flag is True:
                            continue
                        else:
                            server_URL = "http://" + server + "/kvs/update_view"
                            r = requests.put(server_URL, data={ 'change_type': change_type, \
                                'view': os.environ['VIEW'], 'arr': json.dumps(arr,sort_keys=True), 'range_dict': json.dumps(range_dict,sort_keys=True), \
                                'find_hash_dict': json.dumps(find_hash_dict,sort_keys=True), 'partition_id_list': json.dumps(partition_id_list,sort_keys=True), \
                                'partition_members_per_id': json.dumps(partition_members_per_id,sort_keys=True), 
                                'partition_id_finder': json.dumps(partition_id_finder,sort_keys=True), 'K': os.environ['K']
                                })            
            if dead_node_flag is False:
                # Delete all the keys from inputted node
                server_URL = "http://" + ip_port + "/kvs/remove_keys"
                r = requests.put(server_URL, data={'type': 'dict'})
            return jsonify(msg='success', number_of_partitions=len(partition_id_list)), 200

@app.route('/kvs/return_this_guys_keys_and_values', methods=['GET'])
def return_this_guys_keys():
    k = []
    for key, value in storage.items():
        temp_list = [key,value]
        k.append(temp_list)
    return json.dumps(k)

#Dictionary return
@app.route('/kvs/return_this_storage', methods=['GET'])
def return_this_storage():
    k = {}
    for key, value in storage.items():
        k[key] = value
    return json.dumps(k)
   
@app.route('/kvs/update_view', methods=['PUT'])
def update_view():
    global nodes
    global arr
    global range_dict
    global find_hash_dict
    global partition_id_list
    global partition_members_per_id
    global partition_id_finder
    global partition_id

    if request.method == 'PUT':
        try:
            # Update all global arrays
            os.environ['VIEW'] = request.form['view']
            os.environ['K'] = request.form['K']

            nodes = os.environ['VIEW'].split(",")
            arr = json.loads(request.form['arr'])
            arr.sort()
            partition_id_list = json.loads(request.form['partition_id_list']) 
            partition_id_list.sort()

            # Grab dictionaries and parse them into proper dictionary format
            new_range_dict = json.loads(request.form['range_dict'])
            range_dict = {int(k):v for k,v in new_range_dict.items()}

            new_find_hash_dict = json.loads(request.form['find_hash_dict'])
            find_hash_dict = {int(k):v for k,v in new_find_hash_dict.items()}

            new_partition_members_per_id = json.loads(request.form['partition_members_per_id'])
            partition_members_per_id = {int(k):v for k,v in new_partition_members_per_id.items()}

            new_partition_id_finder = json.loads(request.form['partition_id_finder'])
            partition_id_finder = {k:int(v) for k,v in new_partition_id_finder.items()}

            partition_id = partition_id_finder[os.environ['ip_port']]
            return jsonify(msg='update_view: success'), 200
        except Exception as e:
            return jsonify(msg='Error: update_view'), 400


@app.route('/kvs/remove_keys', methods=['PUT'])
def remove_keys():
    type_ = request.form['type']
    if type_ == 'list':
        try:
            keys = json.loads(request.form['keys'])
            for key in keys:
                storage.pop(key)
            return jsonify("removed_keys on list: success"), 200
        except Exception as e:
            return jsonify(msg="error in remove_keys"), 400

    if type_ == 'dict':
        storage.clear()
        return jsonify(msg="removed_keys on entire dict: success"), 200


@app.route('/kvs/forward_request', methods=['PUT'])
def forward_request():
    try:
        ip_port = request.form['ip_port']
    except Exception as e:
        return jsonify(msg="Error: forward_request"), 400
    rkey = []
    rvalue = []
    for key, value in storage.items():
        rkey.append(key)
        rvalue.append(value)

    server_URL = "http://" + ip_port + "/kvs/replicate_node"
    for i in range(0, len(rkey)):
        r = requests.put(server_URL, data={'key': rkey[i], 'value': rvalue[i][0], \
                                            'causal_payload': rvalue[i][1], 'timestamp': rvalue[i][2]})  
    return jsonify(msg='forward_request: success'), 200


@app.route('/kvs/redistribute', methods=['PUT'])
def redistribute():
    # Temporary arrays
    rkey = []
    rvalue = []
    flag = False

    if request.method == 'PUT':
        try:
            change_type = request.form['change_type']
        except Exception as e:
            return jsonify(msg="redistribute error: need to specify valid type 'add' or 'remove'"), 400
        if change_type == 'add':
            hash_location = int(request.form['hash_location'])
            for key, value in storage.items():
                key_hash_location = consistent_hash(key)
                if key_hash_location < hash_location:
                    rkey.append(key)
                    rvalue.append(value)

            for key in rkey: 
                storage.pop(key)

            # Then remove keys for the other nodes in old partition
            for server in partition_members_per_id[partition_id]:
                if server == os.environ['ip_port']: continue
                server_URL = "http://" + server + "/kvs/remove_keys"
                r = requests.put(server_URL, data={'type': 'list', 'keys': json.dumps(rkey)})

            # Insert removed keys into nodes that are in the same partition as the new node (using its hash_location)
            for server in partition_members_per_id[range_dict[hash_location]]:
                server_URL = "http://" + server + "/kvs/replicate_node"
                for i in range(0, len(rkey)):
                    r = requests.put(server_URL, data={'key': rkey[i], 'value': rvalue[i][0], \
                                                        'causal_payload': rvalue[i][1], 'timestamp': rvalue[i][2]}) 

        if change_type == 'remove':
            successor_partition = int(request.form['successor_partition'])
            for key, value in storage.items():
                key_hash_location = consistent_hash(key)
                rkey.append(key)
                rvalue.append(value)  

            # Remove all keys from node-to-be-deleted
            for key in rkey:
                storage.pop(key)
            storage.clear()

            # Redistribute these keys to all nodes in the successor partition
            for server in partition_members_per_id[successor_partition]:
                server_URL = "http://" + server + "/kvs/replicate_node"
                for i in range(0, len(rkey)):
                    r = requests.put(server_URL, data={'key': rkey[i], 'value': rvalue[i][0], \
                                                        'causal_payload': rvalue[i][1], 'timestamp': rvalue[i][2]}) 
        return jsonify(msg='successful redistribution'), 200


@app.route('/kvs/get_number_of_keys', methods=['GET'])
def get_number_of_keys():
    return jsonify(count=len(storage)), 200

@app.route('/kvs/get_partition_id', methods=['GET'])
def get_partition_id():
    global partition_id
    return jsonify(msg="success", partition_id=partition_id, partition_members=partition_members_per_id), 200

@app.route('/kvs/get_all_partition_ids', methods=['GET'])
def get_all_partition_ids():
    global partition_id_list
    return jsonify(msg="success", partition_id_list=partition_id_list), 200

@app.route('/kvs/get_partition_members', methods=['GET'])
def get_partition_members():
    global partition_members_per_id
    p = int(request.args['partition_id'])
    return jsonify(msg="success", partition_members=partition_members_per_id[p], range_dict=range_dict, nodes=nodes, arr=arr), 200

@app.route('/kvs/return_info', methods=['GET'])
def return_info():
    return jsonify(view=os.environ['VIEW'], arr=arr, range_dict=range_dict, find_hash_dict=find_hash_dict, partition_id_list=partition_id_list, \
                    partition_members_per_id=partition_members_per_id, partition_id_finder=partition_id_finder)


if __name__ == '__main__':
    limit = 1000000
    storage = {}
    # global dictionary that maps position on consistent hashing cirle to the partition id's
    range_dict = {}
    # global dictionary that maps partition id's to position on consistent hashing circle
    find_hash_dict = {}
    # global arrary that holds the list of partition_id's
    partition_id_list = []
    # global dict that maps partition id with a list of their partition members
    partition_members_per_id = {}
    try:
        nodes = os.environ['VIEW'].split(",")
        num_replicas = int(os.environ['K'])
        global_num_partitions = int(len(nodes) / num_replicas)      # number of partitions in the system
      
        # create consistent hash unit circle based on partition id's
        arr = make_check_points()
        # assign partition id to all nodes using a dictionary: key=ip_address&value=partition_id
        partition_id_finder = partition_id_assigner()
        # partition id for this node
        partition_id = partition_id_finder[os.environ['ip_port']]
    except Exception as e:   # special case if the input is trying to add/delete a new node in the network
        nodes = []
        arr = []
        partition_id_finder = {}
        partition_id = -1
    thread = threading.Thread(target=intialize_background)
    thread.daemon = True
    thread.start()
    app.run(host='0.0.0.0',port=8080)
