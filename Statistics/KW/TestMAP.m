function [Predictions] = TestMAP(PredictionsValue)
%take difference of each Value with 1 and -1
MappingValue1 = abs(PredictionsValue - 1);
MappingValueMinus1 = abs(PredictionsValue + 1);
for i=1:size(PredictionsValue)
    if MappingValue1(i) < MappingValueMinus1(i)
        Predictions(i,1) = 1;
    else
        Predictions(i,1) = -1;
    end
end
end
