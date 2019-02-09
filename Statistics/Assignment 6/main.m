close all;clc;clear;
%% For 2 images of our own choice

X = corruptImage('cat.jpg');
missData = isnan(X);
[r,c] = find(missData==1);

for i = 1:length(r)
   
   X(r(i),c(i)) =  127;
   
end
for epoch = 1:50
[U,S,V] = svds(X,10);

Xest = U*S*V';

for i = 1:length(r)
   X(r(i),c(i)) =  Xest(r(i),c(i));
end
end
figure, imagesc(X),colormap gray;

%% For 2 given images

% dino = imread('dino.jpg');
% X = rgb2gray(dino);
% figure, imshow(X);
% [r,c] = find(X==0);
% 
% X=im2double(X);
% 
% for i = 1:length(r)
%    
%    X(r(i),c(i)) =  127.5;
%    
% end
% 
% for epoch = 1:50
% [U,S,V] = svds(X,10);
% 
% Xest = U*S*V';
% 
% for i = 1:length(r)
%    
%    X(r(i),c(i)) =  Xest(r(i),c(i));
%    
% end
% end
% figure, imshow(X)

