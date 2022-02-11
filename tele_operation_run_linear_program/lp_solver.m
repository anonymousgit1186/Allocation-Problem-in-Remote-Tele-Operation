fname = 'normal_and_pjt';

val = csvread(strcat(strcat('results/vals_', fname), '.csv'));
I = csvread(strcat(strcat('results/I_', fname), '.csv'))+1;
J = csvread(strcat(strcat('results/J_', fname), '.csv'))+1;
b = csvread(strcat(strcat('results/b_', fname), '.csv'));
c = csvread(strcat(strcat('results/c_', fname), '.csv'));
A = sparse(I,J, val);


options = optimoptions(@linprog,'Display', 'iter')
[x, fval] = linprog(c,A,b,[], [], zeros(size(c)), ones(size(c)), [], options);


csvwrite(strcat(strcat('results/LPval_', fname), '.csv'), fval);
csvwrite(strcat(strcat('results/X_', fname), '.csv'), x);
