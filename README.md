                                                         
                            _/_/    _/_/_/  _/      _/   
                         _/    _/    _/    _/_/  _/_/    
                        _/_/_/_/    _/    _/  _/  _/     
                       _/    _/    _/    _/      _/      
                      _/    _/  _/_/_/  _/      _/       

                          (Analytics in Motion)

-----------------------------------------------------------------------------------------------------------

# Huawei-AIM Benchmark
The Huawei-AIM Benchmark abstracts an event-processing and real-time analytics usecase of the telecommunication industry.
It is a mixed workload consisting of an event processing transaction and seven parametrized simple analytical queries.
More details of the benchmark can be found in the [Analytics in Motion](http://people.inf.ethz.ch/braunl/AIM-SIGMOD-2015.pdf) paper.

## Building
First, build [Tell](https://github.com/tellproject/tell). Then, clone then newest version of the AIM benchmark and build Tell again. This will also build the AIM benchmark:

```bash
cd <tell-main-directory>
cd watch
git clone https://github.com/tellproject/aim-benchmark.git
cd <tell-build>
make
```

### Building for Kudu
If you want to run the AIM Benchmark not only on Tell, but also on [Kudu](http://getkudu.io), first make sure that you configure the kuduClient_DIR correctly in the CMakeLists.txt. Once it is set correctly, you have to call cmake again in the tell build directory and set the addttional flag:

```bash
-DUSE_KUDU=ON
```

## Running
The simplest way to run the benchmark is to use the [Python Helper Scripts](https://github.com/tellproject/helper_scripts). They will not only help you to start TellStore, but also one or several AIM servers and one or several AIM clients.

### Server
Of course, you can also run the server from the commandline. Depending on whether you want to use Tell or Kudu as the storage backend, execute one of the two lines below. This will print out to the console the commandline options you need to start the server:

```bash
watch/aim-benchmark/aim_server -h
watch/aim-benchmark/aim_kudu -h
```

### Clients
There are two different kind of clients in the AIM benchmark. Usually it is enough to start one of each kind (but with potentially more than one thread). The "Stream and Event Processing" (SEP) client uses a UDP connection to send events to the AIM server to be processed there, while the "Real-Time Analytics" (RTA) client sends analytical queries to be processed using TCP. Both clients write log files in CSV format. While the SEP client just logs how many events it was able to send, the RTA client logs every query that was executed with query type, start time, end time (both in millisecs and relative to the beginning of the experiment) as well as whether the queries were answered successfully or not. The clients can connect to AIM servers regardless of the used storage backend. You can find out about the commandline options for these clients by typing:

```bash
watch/aim-benchmark/sep_client -h
watch/aim-benchmark/rta_client -h
```
