# frozen_string_literal: true

# Refactorer: Alex Kuzmenko; Initial Creator: Dmitriy Buzina
# Updated by Alberto Egurrola - January 2023

def normalize_business_indv(name, indv)
  res = []
  name = general_normalize(name)
  name.split(' ').each_with_object(res) do |word, obj|
    word = word.strip.squeeze(' ')
    word = mac_mc(mega_capitalize(word))
    if DOWNCASE.include?(word.downcase) && !name.match(/\s+a\.?\s+/i)
      if (name =~ /#{word}/i) != 0
        word.downcase!
      end
    elsif (word.match(ABBR_LOCAL) && word.match(NOT_ABBR).nil?) ||
          (word.match((BUSINESS_SUFFIXES_LOCAL)) && word.match(/association|company/i).nil?) ||
          word.match(ROMAN_NR) ||
          word.match(/^[a-z]\.?$/i) ||
          (word.match(STATES) && word.match(/^\S+-\S+$/).nil?)
      word.upcase!
    end
    correct_word =
      if indv
        CORRECTIONS_IND_LOCAL.select { |e| word.match(e) }.values[0]
      else
        CORRECTIONS_NOT_IND_LOCAL.select { |e| word.match(e) }.values[0]
      end
    obj << (correct_word ||= word)
  end
  res = res.select{|e| e.to_s.length > 0}.join(' ')
  res
end

def specific_cleaning(name)
  name.gsub!(/Ace Learning LLC Ace Learning LLC/i, 'Ace Learning LLC')
  name.gsub!(/(\s+[a-zA-Z])\?([a-zA-Z])/i, '\1e\2')
  name
end

def contributors_query(indv)
  # {OPTIONS['where'] ? " AND #{OPTIONS['where']}" : 'date >= \'2020-01-01\''}
  <<~SQL
    SELECT
      id,
      contributor_name AS name
    FROM nc_campaign_contributions
    WHERE
      #{OPTIONS['new_records_only'] ? 'clean_contributor_name is null ' : '(1)'}
      AND type #{indv ? '' : '!'}= 'Individual'
      #{OPTIONS['where'] ? " AND #{OPTIONS['where']}" : ''}
      #{OPTIONS['limit'] ? " LIMIT #{OPTIONS['limit']}" : ''}
    ;
  SQL
end

def clean_algo(indv = true)
  begin
    db13 = C::Mysql.on(DB13, 'nc_raw')

    puts contributors_query(indv)
    contributors = db13.query(contributors_query(indv)).to_a

    semaphore = Mutex.new
    threads = Array.new(5) do
      Thread.new do
        db13_ins = C::Mysql.on(DB13, 'nc_raw')

        loop do
          contributor = nil
          semaphore.synchronize { contributor = contributors.pop }

          break unless contributor

          name = contributor['name']
          next if name.nil?

          if name == 'Aggregated Individual Contribution'
            general_query = <<~SQL
              UPDATE nc_campaign_contributions
              SET clean_contributor_name = #{name.dump}
              WHERE id = #{contributor['id']};
            SQL
            db13_ins.query(general_query)
            next
          end

          puts "#{contributor['id']} id is processing"

          new_name = name.delete('*')
          new_name = specific_cleaning(new_name)
          new_name = new_name.gsub(/\A\s*[-.,:]+/i, '')
          new_name = new_name.gsub(/\s*[('"][^)'"]+[)'"]\s*/i, ' ') #nicknames inside (), '' or ""
          new_name = new_name.gsub(/\s+Alias\s+.+/i, '') # remove alias
          new_name.gsub!(/\s{2,}/i, ' ')
          new_name = new_name.strip
          new_name = normalize_business_indv(new_name, indv)
          new_name = new_name.split(' ').map do |word|
            word.length == 1 && word != '/' && word != '&' ? "#{word}." : word
          end.join(' ')
          new_name = new_name.gsub(/(\()([^)]+)$/, '')

          puts "name: #{name} || new_name: #{new_name}"

          update_query = <<~SQL
            UPDATE nc_campaign_contributions
            SET clean_contributor_name = #{new_name.dump}
            WHERE id = #{contributor['id']};
          SQL
          db13_ins.query(update_query)
        end # loop
        db13_ins.close if db13_ins
      end # Thread
    end # Threads
    threads.each(&:join)
  rescue => e
    puts "#{e} ~> #{e.backtrace.join("\n")}"
  ensure
    db13.close if db13
  end
  puts 'Done'
end

def contributors_cleaning
  puts OPTIONS
  clean_algo(true)
  clean_algo(false)
end
