# Random UChile Source Verifier

This project allows to verify Random UChile pulses, collecting and comparing the data from the public sources used by the beacon to generate its pulse.

It also generates a list of json files with the results of each comparison.

# Requirements

* Python 3.7 or higher
* `requirements.txt` packages

# How to use

* `pip install -r requirements.txt`
* Create a file named `config.json` on this folder. You can use `config.json.sample` as base. 
* Edit the config file using the parameters provided by the beacon specification.
* Add personal APIs to the config file
* Personalize your result and log routes if you want.
* Run the verifier with `python main.py`.

# Verification Specification

* Check the [Wiki](https://github.com/clcert/beacon-source-verifier/wiki/) for more information.

## TODO
* Ethereum Verifier
* Seismology Verifier
