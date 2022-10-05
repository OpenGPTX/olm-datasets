# Analyze the Common Crawl or Wikipedia pretraining datasets that you created

This folder has a tool that you can use to get a little information about how recent your data is. We expect to add to it with time.

## To analyze for date mentions in the text

```
python date_analysis.py --input_dataset_name=Tristan/olm-wikipedia20220920 --text_column=text --split=train --num_proc=96 --load_from_hub_instead_of_disk
```

## Documentation

```
python date_analysis.py --help
```