% Submitted by Hussam-Ul-Hussain
% Roll no: 18L-1827

close all; clc; clear;

trainX = load('ocr3_7train.txt');

trainY = load('ocr3_7trainLabels.txt');

testX = load('ocr3_7test.txt');

testY = load('ocr3_7testLabels.txt');

for i = 1:4

    out = [ones(size(trainX,1),1) trainX];

    if i == 1
        lambda = 0.1;
    elseif i == 2
        lambda = 0.001;
    elseif i == 3
        lambda = 1e-3;
    else
        lambda = 0.035;
    end

regressionCoefficients = trainRegression(out,trainY,lambda);

outTest = [ones(size(testX,1),1) testX];

TrainingPredictions = testRegression(out,regressionCoefficients);

TestingPredictions = testRegression(outTest,regressionCoefficients);

TrainingPredictions = MapTest(TrainingPredictions);

TestingPredictions = MapTest(TestingPredictions);

[TP, FP, TN, FN] = confusionMatrix(TrainingPredictions,trainY);

fprintf("For Lambda %f\nResult For Training Data\nTP = %d\nTN = %d\nFP = %d\nFN = %d\n",lambda,TP,TN,FP,FN);

disp("------------------------------------------------------------------");

[TP, FP, TN, FN] = confusionMatrix(TestingPredictions,testY);
fprintf("For Lambda %f\nResult For Test Data\nTP = %d\nTN = %d\nFP = %d\nFN = %d\n",lambda,TP,TN,FP,FN);

disp("--------------------------END ITERATION---------------------------");

end