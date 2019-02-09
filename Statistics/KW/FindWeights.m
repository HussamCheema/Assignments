function [Weights,W0] = FindWeights(alphas, X, target, thrshld)
%%Finding Weights 
xWights = 0;
randomAlpha = 0;
n = 0;
size = 0;
for i = 1:length(X)
    xWights = xWights + alphas(i)*target(i)*X(i,:)';
end
Weights = xWights;
for i = 1:length(X)
    if(alphas(i) > thrshld)
        randomAlpha(i) = 1;
    else
        randomAlpha(i) = -1;
    end
end
for t = 1:length(X)
    if(randomAlpha(t) == 1)
        n = n + (target(t) - square(target(t))*Weights'*X(t,:)')/target(t);
        size = size + 1;
    end
end
W0 = n/size;
end

