
with open("X_final_normal_and_pjt.csv", "r") as xf:
	cnt= 0
	first = True
	for line in xf:
		if not first:
	
			val = float(line.strip().split(",")[-1])
			if val>0:
				cnt+=1		
		else:
			first = False

print(cnt)
