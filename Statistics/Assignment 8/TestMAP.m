function [testingPredictions] = TestMAP(TestingPredictions)

    DifferenceWith1 = abs(TestingPredictions - 1);
    DifferenceWithMinus1 = abs(TestingPredictions + 1);
    
    for i=1:size(TestingPredictions)

        if DifferenceWith1(i) < DifferenceWithMinus1(i)
            testingPredictions(i,1) = 1;
        else
            testingPredictions(i,1) = -1;
        end
                
    end
    
end
