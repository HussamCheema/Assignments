function [alphas] = SVMObjFunction(alphas,X,target)
    
    x = 0;

    for i = 1:200
        for j = 1:200
            x = x + alphas(i)*alphas(j)*target(i)*target(j)*((X(i,:)*X(j,:)')) - alphas(i);
        end
    end
    
    alphas = x/2;
end

