function [TP, FP, TN, FN] = confusionMatrix(prediction,actualLabels)

TP = 0; TN = 0; FP = 0; FN = 0;

for i = 1:size(prediction(:,1))

    if prediction(i) == 1
        if actualLabels(i) == 1
            TP = TP + 1;
        elseif actualLabels(i) == -1
            FP = FP + 1;
        end
    end

    if prediction(i) == -1
        if actualLabels(i) == 1
            FN = FN + 1;
        elseif actualLabels(i) == -1
            TN = TN + 1;
        end
    end

end


end

