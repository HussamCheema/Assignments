function t = SVMObjFunction(alphas,X,target)
   sz = size(alphas,1);
   t = 0;
   for i=1:sz
       for j=1:sz
           t = t + alphas(i)*alphas(j)*target(i)*target(j)*((X(i,:)*transpose(X(j,:))))-alphas(i);
       end
   end
   
    t= t/2;
           
end