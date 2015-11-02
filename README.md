                                                         
                            _/_/    _/_/_/  _/      _/   
                         _/    _/    _/    _/_/  _/_/    
                        _/_/_/_/    _/    _/  _/  _/     
                       _/    _/    _/    _/      _/      
                      _/    _/  _/_/_/  _/      _/       

                          (Analytics in Motion)

-----------------------------------------------------------------------------------------------------------

I. AIM Server
=============
server/: contains storage, Stream & Event Processing (SEP) and RealTime Analytics (RTA).
For more information on how to start the server please refer to the README file located at server/README.

2. SEP-client
=============
sep-client/: contains client code that sends events to the server.
For more information on how to start sep-client please refer to the README file located at sep-client/README.

3. RTA-client
=============
rta-client/: contains client code that sends RTA queries to the server.
For more information on how to start rta-client please refer to the README file located at rta-client/README.

4. How to build
===============
Check the README files located in the project folders listed above.

5. How to run
=============
# You should first run the aim server.
# When population is over, sep and rta client can also be started.

Alternatively, you can use the automated testing framework located in framework/.

6. Miscalenous
==============
- AIMSchemaCampatignGenerator/: used to generate an AIM wide table schema with a specific number of attributes and campaigns. A sample SQL file is provided.
- util/: util classes that are used in different parts of the code.  	
