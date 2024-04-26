# Collocation Extraction with AWS Elastic MapReduce

## Overview

This Java project utilizes the AWS SDK for Java, specifically Amazon Elastic MapReduce (EMR), to extract collocations from the Google 2-grams dataset. Collocations are pairs of words that frequently co-occur together in a given context, indicating potential linguistic associations.

The project further calculates the Normalized Pointwise Mutual Information (NPMI) for each bigram in each decade, as well as the decade-relative NPMI for each bigram. NPMI is a statistical measure used to assess the strength of association between two words, considering the frequency of their co-occurrence relative to their individual frequencies.

## AWS Elastic MapReduce Setup

To run the Map-Reduce jobs on AWS EMR, ensure the following setup:
- Configure an EMR cluster with appropriate instance types and configurations.
- Package the Java project along with the AWS SDK into a JAR file and upload it to Amazon S3.
- Submit the JAR file as a step to the EMR cluster, specifying the input and output paths for each step.

## Parameters and Thresholds

- NPMI Threshold: 0.5
- Decade-Relative NPMI Threshold: 0.2
- Bigrams are filtered based on these thresholds to retain only significant associations.

## Output Format

The final output consists of filtered bigrams sorted by decade and NPMI values in decreasing order, ready for further analysis or visualization.

## Dependencies

- AWS SDK for Java
- Amazon Elastic MapReduce (EMR)
- Google 2-grams dataset

## Notes

- Set up appropriate IAM roles and permissions for accessing AWS services.
- Monitor EMR clusters for optimal performance and cost efficiency.
- Adjust thresholds and parameters as needed based on specific requirements and dataset characteristics.
