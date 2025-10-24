# Known Issues

The purpose of the load generator is to accurately simulate performance data sent from the agent to the CDT server.

Current issues:

1. Not all data types from the TCP dump are sent.  
   **XML**, **SQL**, **traces**, and **suspends** are currently ignored.  
   Only **params**, **dictionaries**, and **calls** are sent.  
   The main challenge is maintaining correct timing to mimic real agent behavior.  
   For example, **traces** and **calls** must be sent simultaneously throughout the test.

2. Only one TCP dump is used.  
   In real scenarios, different services generate different loads.  
   Currently, all pods in a test generate the same traffic, which does not reflect realistic load diversity.
