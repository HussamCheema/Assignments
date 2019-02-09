function [wts] = trainGradientDescent(Xtrain, Ytrain, LR, momentum)

    deltaW = zeros(1,257);
    W = randn(1,257);
    m = length(Xtrain);
    
    % Iterations
    for k = 1:100
        
        %% Training the model to get Regression Coefficients
        % Iteration for Epochs
        for i = 1 : m
            X = Xtrain(i,:);
            predicted_output = (W * X');
            deltaW = (momentum*deltaW) + (LR/m)*(Ytrain(i,1) - predicted_output)*X;
            W = W + deltaW;
        end

    end
    
    wts = W;
    
end

