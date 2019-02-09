function [alphas] = SVMObjFunction(alphas,X,target)
[Row,Column] = size(X);
Value = 0;
for i = 1:Row
    for j = 1:Row
        Value = Value + alphas(i)*alphas(j)*target(i)*target(j)*((X(i,:)*X(j,:)')) - alphas(i);
    end
end
alphas = Value/2;
end

