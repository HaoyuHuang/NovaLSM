import math

from scipy.stats import binom 


def ncr(n,r):
    f = math.factorial
    return f(n) / f(r) / f(n-r)

# setting the values 
# of n and p 
beta = 10
p = 1.0/(4.3*30*24)#-math.exp(-1/(4.3))
print p
# obtaining the mean and variance  
mean, var = binom.stats(beta, p) 
# list of pmf values 
# printing mean and variance 
print("mean = "+str(mean)) 
print("variance = "+str(var))

for beta in [10]:
	print "beta={}".format(beta)
	for rho in [1, 3, 5, 10]:
		availability=0.0
		for k in range(0, beta+1):
			a=0
			if beta-k >= rho:
				a=float(ncr(beta-k, rho))/float(ncr(beta, rho))
			failure=binom.pmf(k, beta, p)
			availability+=a*failure
		print "{},{},{},{}".format(beta, rho, availability, 1/(1.0-availability))
	# print "{},{},{},{}".format(beta,beta, pow(1.0-p, beta), 1/(1.0-pow(1.0-p, beta)))


# RAID5.
beta=10
mttf=4.3*30*24
mttr=1
for rho in [1]:
	sstable_mttf=mttf*mttf/(rho*(rho+1)*mttr)
	system_mttf=sstable_mttf/(beta/rho)
	print "{},{},{}".format(rho, sstable_mttf, system_mttf)