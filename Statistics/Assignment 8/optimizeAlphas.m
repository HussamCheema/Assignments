function [alphas] = optimizeAlphas(alpha, X, target, UB)

    Beq = zeros(length(X),1);
    Aeq = zeros(length(X),length(X));
    Aeq(1,:) = target;
    b = [];   
    A = [];
    x0 = alpha;
    ub = ones(200,1)*UB;
    lb = zeros(length(X),1);
    objfunc = @(alphas)SVMObjFunction(alpha,X,target); 

    alphas = fmincon(objfunc,x0,A,b,Aeq,Beq,lb,ub);

end

