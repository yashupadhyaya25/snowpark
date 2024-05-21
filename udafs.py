class sales_by_date():
   def __init__(self):
     self._partial_sum = 0

   @property
   def aggregate_state(self):
     return self._partial_sum

   def accumulate(self, input_value):
     self._partial_sum += input_value

   def merge(self, other_partial_sum):
     self._partial_sum += other_partial_sum

   def finish(self):
     return self._partial_sum