function [Weights,W0] = FindWeights(alphas, X, target, alpha_threshold)
    
    x = 0;
    new_alphas = 0;
    
    for i = 1:length(X)
        x = x + alphas(i)*target(i)*X(i,:)';
    end
    
    Weights = x;
    
    % Mapping alphas to 1 and -1 by setting alpha_threshold
    for i = 1:length(X)
       if(alphas(i) > alpha_threshold)
           new_alphas(i) = 1;
       else
           new_alphas(i) = -1;
       end
    end
    
    n = 0;
    size = 0;
    
    for t = 1:length(X)
        %Computing W0 only for Support Vectors where alpha = 1.
        if(new_alphas(t) == 1)
            n = n + (target(t) - square(target(t))*Weights'*X(t,:)')/target(t);
            size = size + 1;
        end
    end
    
    %taking average
    W0 = n/size;
    
end

