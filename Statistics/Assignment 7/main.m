%%Submitted By: Hussam-Ul-Hussain | Roll # 18L-1827

close all;clc;clear;

for k = 1:4

    trainX = load('ocr3_7train.txt');
    trainY = load('ocr3_7trainLabels.txt');
    testX = load('ocr3_7test.txt');
    testY = load('ocr3_7testLabels.txt');
    trainX = [ones(size(trainX,1),1) trainX];
    testX = [ones(size(testX,1),1) testX];
    
    if(k == 1)
        LR = 0.1;
        momentum = 0.5;
    elseif(k == 2)
        LR = 0.7;
        momentum = 0.9;
    elseif(k == 3)
        LR = 0.0001;
        momentum = 0.8;
    else
        LR = 0.00001;
        momentum = 0.7;
    end

    [Weights] = trainGradientDescent(trainX, trainY, LR, momentum);

    [TestPredictions] = testGradientDescent(testX, Weights);

    [TrainPredictions] = testGradientDescent(trainX, Weights);

    %% Confusion Matrix

    [TrainingPredictions] = TestMAP(TrainPredictions);

    [TestingPredictions] = TestMAP(TestPredictions);

    fprintf("------------------------Result %d---------------------------\n",k);
    fprintf("Learning Rate = %f AND Momentum = %f\n",LR,momentum);
    [TP, FP, TN, FN] = confusionMatrix(TrainingPredictions,trainY);
    BAC = (TP/(TP+FN)+TN/(TN+FP))/2;
    fprintf("\nResult For Training Data\nTP = %d\nTN = %d\nFP = %d\nFN = %d\n",TP,TN,FP,FN);
    fprintf("\nBalanced Accuracy = %f\n",BAC);
    disp("------------------------------------------------------------------");

    [TP, FP, TN, FN] = confusionMatrix(TestingPredictions,testY);
    BAC = (TP/(TP+FN)+TN/(TN+FP))/2;
    fprintf("\nResult For Test Data\nTP = %d\nTN = %d\nFP = %d\nFN = %d\n",TP,TN,FP,FN);
    fprintf("\nBalanced Accuracy = %f\n",BAC);
    disp("--------------------------END ITERATION---------------------------\n");

end