import subprocess
import os

from lp_constraints_teleop_data import main_code 
from update_result import update_final_results
main_code(3600,10,3)
s = subprocess.check_output(["matlab","-batch", "lp_solver"])
print(s)
update_final_results()
