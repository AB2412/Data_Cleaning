# Refactorer: Alex Kuzmenko; Creator: Git - Proserge

def get_recent_date(db13)
  query = <<~SQL
    SELECT MAX(scrape_date) AS recent_date
    FROM nc_campaign_recipient_names_clean;
  SQL
  result = db13.query(query).to_a.first['recent_date']

  result ? result.to_s : Date.new(2020, 1, 1).to_s
end

def get_names_to_clean(recent_date)
  <<~SQL
    SELECT DISTINCT
      recipient_name AS name,
      created_at AS scrape_date
    FROM nc_campaign_expenditures
    WHERE 
      (1)
      #{OPTIONS['where'] ? " AND #{OPTIONS['where']}" : " AND created_at BETWEEN #{recent_date.dump} AND #{Date.today.to_s.dump}"}
      AND recipient_name IS NOT NULL
    GROUP BY name
      #{OPTIONS['limit'] ? " LIMIT #{OPTIONS['limit']}" : ''}
  SQL
end

def recipients_cleaning
  puts OPTIONS
  begin
    db13 = C::Mysql.on(DB13, 'nc_raw')

    recent_date = get_recent_date(db13)
    puts get_names_to_clean(recent_date)
    names_to_clean = db13.query(get_names_to_clean(recent_date)).to_a

    if names_to_clean.empty?
      puts 'Nothing to clean'.red
      return
    end

    det = F::Determiner.new

    counter = 0
    total = names_to_clean.size
    names_to_clean.each do |row|
      counter += 1
      print "[#{counter}/#{total}]: #{row['name']}"
      clean_name = row
      clean_name['name_type'] = det.determine(row['name'])

      temp_name =
        if clean_name['name_type'] == 'Person'
          F::Cleaner.person_clean(row['name'], false)
        else
          F::Cleaner.org_clean(row['name'])
        end
      temp_name.multi_gsub!('THE ', 'The ').multi_gsub!('Usps ', 'USPS ').multi_gsub!('. Com', '.com')
               .multi_gsub!('Us ', 'US ').multi_gsub!(' LLC.', ' LLC')
      clean_name['name_cleaned'] = temp_name
      clean_name['name_cleaned'] = extra_cleaning(clean_name['name_cleaned'].dup)
      clean_name['skip_it'] = skip(clean_name['name_cleaned'].dup)

      print " || #{clean_name['name_cleaned']}\n"
      # puts clean_name

      insert_query = <<~SQL
        INSERT INTO nc_campaign_recipient_names_clean (#{clean_name.keys.map { |e| "`#{e}`" }.join(', ')})
        VALUES (#{clean_name.values.map { |e| "#{escape(e).dump}" }.join(', ')})
        ON DUPLICATE KEY UPDATE #{clean_name.map { |k, v| "#{k} = values(#{k})"}.join(', ')}
        ;
      SQL
      # puts insert_query
      db13.query(insert_query)
      # DB.insert_query(DB13, 'nc_raw', 'nc_campaign_recipient_names_clean', clean_name, mode = 'update', { 'verbose' => 'disabled'})
    end
  rescue => e
    puts "#{e} ~> #{e.backtrace.join("\n")}"
  ensure
    db13.close if db13
  end
  puts 'Done'
end

def skip(name)
  puts "--"
  skip_it = !SKIPS.select { |e| name.match(e) }.values.empty? ? SKIPS.select { |e| name.match(e) }.values[0] : 0
  puts skip_it
  puts "--"
  skip_it
end

def extra_cleaning(name)
  name.gsub!(/(\d)\s*\.\s*(\d+)/i, '\1.\2')
  name.gsub!(/\\+'/, '\'')
  name.gsub!(/\A\s*'([^']+)$/i, '\1')
  name.gsub!(/\A\s*(\.\s*)+/i, '')
  name.gsub!(/(^\s*|\s+)0+([1-9][0-9]*(st|nd|rd|th))/i, '\1\2')
  name.gsub!(/DR\./, 'Dr.')
  name.gsub!(/WWW\s*\.\s+(\S+)\s*(\.com)\s*(\S+)?/i, 'www.\1\2\3')
  name.strip.squeeze(' ')
end
