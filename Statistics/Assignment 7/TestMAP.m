function [testingPredictions] = TestMAP(TestingPredictions)

    DifferenceWith7 = abs(TestingPredictions - 7);
    DifferenceWith3 = abs(TestingPredictions - 3);
    
    for i=1:size(TestingPredictions)

        if DifferenceWith7(i) < DifferenceWith3(i)
            testingPredictions(i,1) = 7;
        elseif DifferenceWith3(i) < DifferenceWith7(i)
            testingPredictions(i,1) = 3;
        end
                
    end
    
end

