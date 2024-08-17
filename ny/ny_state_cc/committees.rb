# Creator: Alex Kuzmenko; Based on file created by "proserge" - GIT

CLEAN_COMMITTEE_TABLE = 'ny_campaign_finance_committees__clean'

def manual_commitee_corrections
  {
    /\bCOMM(?:\.|\b)/i => 'Committee',
    /\bLEG(?:\.|\b)/i => 'Legislature',
    /\bGOV\.T\b/i => 'Government',
    /\bNyc\b/ => 'NYC',
    /\bNys\b/ => 'NYS',
    / a\. / => ' A. ',
    / DR\. / => ' Dr. ',
    /\bJR(?:\.|\b)/ => 'Jr.',
    /\bMcc(?=[a-zA-z])/ => 'McC',
    /\bMcg(?=[a-zA-z])/ => 'McG',
    /\bMcm(?=[a-zA-z])/ => 'McM',
    /\bMce(?=[a-zA-z])/ => 'McE',
    /\bDa\b/ => 'District Attorney',
    /\b(Ad|A\. D\.)(?=\s|$)/ => 'Assembly District'
  }
end

def recent_date_query
  <<~SQL
    SELECT MAX(scrape_date) AS recent_date
    FROM #{CLEAN_COMMITTEE_TABLE};
  SQL
end

def committees_query(recent_date)
  <<~SQL
    SELECT
      filer_name AS committee_name,
      DATE(updated_at) AS scrape_date
    FROM ny_campaign_finance_committees
    WHERE DATE(updated_at)>= #{recent_date.dump};
  SQL
end

def committees_cleaning
  begin
    db01 = C::Mysql.on(DB01, 'usa_raw')

    recent_date_data = db01.query(recent_date_query).to_a
    recent_date = recent_date_data.first['recent_date'].to_s || Date.new(2020, 1, 1).to_s

    committees_to_clean = db01.query(committees_query(recent_date)).to_a

    committees_to_clean.each do |row|
      raw_name   = row['committee_name']
      raw_scrape = row['scrape_date'].to_s

      clean_name = F::Cleaner.org_clean(raw_name)
      manual_commitee_corrections.each_pair do |k, v|
        clean_name = clean_name.sub(k, v)
      end
      clean_name = clean_name.strip.squeeze(' ')

      insert_query = <<~SQL
        INSERT IGNORE INTO #{CLEAN_COMMITTEE_TABLE} (committee_name, committee_name_cleaned, scrape_date)
        VALUES (#{raw_name.dump}, #{clean_name.dump}, #{raw_scrape.dump});
      SQL
      puts insert_query.green
      db01.query(insert_query)
    end
  rescue Exception => e
    p "Something went wrong"
    p e
  ensure
    db01.close if db01
  end
  puts 'Done'
end
