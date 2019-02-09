function [Predictions] = GetPridictions(testX, Weights, W0)

    Predictions = (testX*Weights) + W0;

end

