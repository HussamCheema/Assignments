%%Submitted By: Hussam-Ul-Hussain | Roll # 18L-1827

close all;clc;clear;
Data = load('SVMData.txt');
x_train = Data(:,1:2);
y_train = Data(:,3);
testX = x_train(150:200,1:2);
testY = y_train(150:200,1);
alpha_random = randn(200,1);

UpperBounds = [10,1,5,100,-1];
alpha_threshold = [1e-5,1e-2,1e-8,1,0];

% Specify index to select UpperBound and corresponding alpha_threshold
% Select index in range of 1 to 5
index = 1;

% This function takes almost 20 seconds to find local minimum that
% satisfies the constraints.
[alphas] = optimizeAlphas(alpha_random, x_train, y_train, UpperBounds(index));

[Weights, W0] = FindWeights(alphas, x_train, y_train, alpha_threshold(index));

[Predictions] = GetPridictions(testX, Weights, W0);

plotSVMData(x_train,y_train,Weights,W0);

disp("Weight Vector: ");
disp(Weights);
fprintf("\nW0: %f\n",W0);

%% Confusion Matrix
[TestingPredictions] = TestMAP(Predictions);

fprintf("------------------------Result ---------------------------\n");

[TP, FP, TN, FN] = confusionMatrix(TestingPredictions,testY);
BAC = (TP/(TP+FN)+TN/(TN+FP))/2;
fprintf("\nResult For Test Data\nTP = %d\nTN = %d\nFP = %d\nFN = %d\n",TP,TN,FP,FN);
fprintf("\nBalanced Accuracy = %f\n",BAC);
