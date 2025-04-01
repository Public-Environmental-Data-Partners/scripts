for prefix in $(ls *_2000.zip | sed 's/_2000.zip//'); do
    zip "${prefix}_all_years.zip" "${prefix}"_*.zip
done
