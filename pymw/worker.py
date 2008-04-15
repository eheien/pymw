from app_types import *

input = pymw_get_input()
output = Output(input.value*input.value)
pymw_return_output(output)

