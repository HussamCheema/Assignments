function [Predictions] = GetPridictions(testX, Weights, W0)
%% Predictions
Predictions = (testX*Weights) + W0;
end

