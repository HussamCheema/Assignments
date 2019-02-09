clc;
clear all;
close all;
Data = load('SVMData.txt');
XInput = Data(:,1:2);
YLable = Data(:,3);
[Row,Column] = size(XInput);
testX = XInput(150:Row,1:2);
testY = YLable(150:Row,1);
alphaRandom = randn(200,1);
%Report Requirments Upper Bound and Alphas
Iterator = 1;
UB = [10,1,15,50,-10];
arrAlphaThreshold = [1e-5,1e-2,1e-6,3,1e-2];
[alphas] = optimizeAlphas(alphaRandom, XInput, YLable, UB(Iterator));
[Weights, W0] = FindWeights(alphas, XInput, YLable, arrAlphaThreshold(Iterator));
[actualPrediction] = GetPridictions(testX, Weights, W0);
plotSVMData(XInput,YLable,Weights,W0);
%%
%Display Data
disp("Weights");
disp(Weights);
disp("W0");
disp(W0);
[TestingPredictions] = TestMAP(actualPrediction);
[TP, FP, TN, FN] = confusionMatrix(TestingPredictions,testY);
BAC = (TP/(TP+FN)+TN/(TN+FP));
BAC = BAC/2;
disp("TP = ");disp(TP);
disp("TN = ");disp(TN);
disp("FP = ");disp(FP);
disp("FN = ");disp(FN);
disp("BAC = "); disp(BAC);
