class full_name() :
    
    def __init__(self) -> None:
        pass
    
    def process(self,first_name,last_name):
        full_name = first_name + ' ' + last_name
        yield (full_name.upper(),)