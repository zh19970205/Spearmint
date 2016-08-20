import numpy as np
import math

def branin(x, y):

    result = np.square(y - (5.1/(4*np.square(math.pi)))*np.square(x) + 
         (5/math.pi)*x - 6) + 10*(1-(1./(8*math.pi)))*np.cos(x) + 10
    
    result = float(result)

    print "param = %d, %d"%(x,y)
    print 'Result = %f' % result
    #time.sleep(np.random.randint(60))
    return [{"x":x,"y":y},result]

# Write a function like this called 'main'
def main(job_id, params):
    x=params["x"]
    y=params["y"]
    return [branin(x,y),branin(x/2.,y/2.)]
