close all;clc;clear;
data = load('SVMData.txt');
X = data(:,1:2);
target = data(:,3);
alphas = optimizeAlphas(X,target);
