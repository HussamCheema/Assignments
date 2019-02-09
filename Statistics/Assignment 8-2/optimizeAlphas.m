function alphas = optimizeAlphas(X,target)
   alphas = randn(size(target,1),1);
   
   objfunc = @(alphas)SVMObjFunction(alphas,X,target); 
   Aeq = zeros(size(target,1),size(target,1));
   Aeq(1,:) = target;
   beq = zeros(size(target,1),1);
   lb(1:size(target,1),1:1) = 0;
   ub(1:size(target,1),1:1) = 5;
   output = fmincon(objfunc,alphas,[],[],Aeq,beq,lb,[]);
   alphas = output;
end
