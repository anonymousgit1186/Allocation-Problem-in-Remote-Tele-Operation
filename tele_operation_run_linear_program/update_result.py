import pickle

def update_final_results():
	fname = 'normal_and_pjt'
	
	with open("results/dict_" + fname +".csv", "rb") as f:
	    dct = pickle.load(f)

	with open("results/X_" + fname +".csv", "rb") as f, open("results/X_final_"+ fname +".csv","w+") as outp :
	    print('t,t_prime,i,j,val', file=outp)
	    for n,line in enumerate(f.readlines()):
	        #print(n, dct[n], line.strip())
	        s1 = "{},{},{},{}".format(dct[n][0],dct[n][1],dct[n][2],dct[n][3])
	        s2 = "," + str(float(line.strip()))

	        print(str(s1)+str(s2), file=outp, flush=True)


	print("Done All")
         
