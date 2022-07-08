# city_of_phl
## Set-Up
**Running this code requires `Python` version >= 3.9 and `pandas` >= 1.4.0**. There are two ways of going about this:  
1. Run the Powershell file `run_main.Ps1` in a Powershell terminal with the command `.\run_main.Ps1`, which will automatically create a virtual environment and run `main.py` automatically, or 
2. Manually run `pip install pandas --upgrade`, and then run `python main.py` from the terminal window. 

The answers from my most recent run are saved in the file `answers.csv`; running `main.py` will overwrite this. 

Note that running running the task will take on the order of 10 minutes. 

## Assumptions / Simplifications: 
* After corresponding with Alex Waldman, I settled on the following definition of a condo from the OPA records dataset: 
    * Residential (i.e. `category_code == 1`)
    * Geometry is a duplicate of another Residential OPA record
    * `building_code_description` contains the word "condo"
* DOR concatenated addresses take the following form: 
    * [HOUSE][SUF]-[STEX] [FRAC] [STDIR] [STNAM] [STDES] [STDESSUF] UNIT [UNIT]